/**
 * LineagePerfPage — perf-benchmark extensions to {@link LineageV2Page}.
 *
 * Adds mouse-driven pan/zoom (exercising the real React Flow event path),
 * node-body click, fanout expansion, and a stable-count waiter. Selector
 * conventions mirror the parent page.
 */

import type { Page, Locator } from '@playwright/test';

import { LineageV2Page } from './lineage-v2.page';
import type { DataHubLogger } from '../utils/logger';

export class LineagePerfPage extends LineageV2Page {
  readonly canvas: Locator;
  readonly viewport: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    // Pan against the outer container, not `.react-flow__viewport` (the inner
    // transform target), so the drag math accounts for the real hit area.
    this.canvas = page.locator('.react-flow').first();
    this.viewport = page.locator('.react-flow__viewport').first();
  }

  /** Drag right-then-left in ~30 px steps so the RF store emits multiple updates. */
  async panHorizontal(distance = 240): Promise<void> {
    const box = await this.canvas.boundingBox();
    if (!box) return;
    const cx = box.x + box.width / 2;
    const cy = box.y + box.height / 2;
    await this.page.mouse.move(cx, cy);
    await this.page.mouse.down();
    for (let dx = 0; dx <= distance; dx += 30) {
      await this.page.mouse.move(cx - dx, cy);
    }
    for (let dx = distance; dx >= 0; dx -= 30) {
      await this.page.mouse.move(cx - dx, cy);
    }
    await this.page.mouse.up();
  }

  /** Discrete wheel-driven zoom. Positive `steps` zooms in, negative out. */
  async zoom(steps: number): Promise<void> {
    const box = await this.canvas.boundingBox();
    if (!box) return;
    const cx = box.x + box.width / 2;
    const cy = box.y + box.height / 2;
    await this.page.mouse.move(cx, cy);
    for (let i = 0; i < Math.abs(steps); i += 1) {
      await this.page.mouse.wheel(0, steps > 0 ? -120 : 120);
      await this.page.waitForTimeout(50);
    }
  }

  async zoomIn(steps = 2): Promise<void> {
    await this.zoom(steps);
  }

  async zoomOut(steps = 2): Promise<void> {
    await this.zoom(-steps);
  }

  async clickNodeBody(urn: string): Promise<void> {
    await this.getNode(urn).click({ force: true });
  }

  /** Best-effort: not every sidebar variant exposes this testid. */
  async closeSidebar(): Promise<void> {
    const closeButton = this.page.locator('[data-testid="entity-profile-sidebar-close"]').first();
    if (await closeButton.count()) await closeButton.click();
  }

  /**
   * Poll for an unchanged node count over `stableMs`. Necessary because
   * `useBulkEntityLineage` fetches in batches of 10 — consecutive samples
   * can briefly agree while the next batch is in-flight.
   */
  async waitForStableNodeCount({
    pollMs = 250,
    maxMs = 5_000,
    stableMs = 1_500,
  }: { pollMs?: number; maxMs?: number; stableMs?: number } = {}): Promise<number> {
    const deadline = Date.now() + maxMs;
    let lastCount = -1;
    let lastChangeAt = Date.now();
    let count = 0;
    while (Date.now() < deadline) {
      count = await this.page.locator('.react-flow__node').count();
      if (count !== lastCount) {
        lastCount = count;
        lastChangeAt = Date.now();
      } else if (count > 0 && Date.now() - lastChangeAt >= stableMs) {
        return count;
      }
      await this.page.waitForTimeout(pollMs);
    }
    return count;
  }

  async waitForReactFlowNode(urn: string, timeoutMs = 15_000): Promise<void> {
    await this.getReactFlowNode(urn).waitFor({ state: 'attached', timeout: timeoutMs });
  }

  /**
   * Repeatedly click filter-node expand affordances until the graph stops
   * growing. Prefers larger steps: `show-max` (+100) > `show-all` > `show-more`
   * (+4). The non-`show-more` buttons live in a hover-revealed menu, so we
   * hover the wrapper first to make them interactive.
   *
   * Virt-awareness: under virt, expand wrappers may be unmounted after
   * each round's fitView. Before bailing on "no wrappers" we zoom out
   * incrementally (up to {@link maxZoomOutTicks}). We also can't trust
   * the DOM count as a "did the graph grow" signal — virt keeps the DOM
   * count bounded by viewport even when the store grew by 100. Instead
   * we use "did this round click any wrappers at all" plus a max round
   * cap. Without this fallback the perf comparison degenerates into
   * "11-node-virt vs 211-node-baseline" because virt's helper bailout
   * happens after one round.
   *
   * Returns the total click count.
   */
  async expandFanoutFully({
    maxRounds = 20,
    settleMaxMs = 8_000,
    settleStableMs = 1_200,
    maxZoomOutTicks = 20,
  }: {
    maxRounds?: number;
    settleMaxMs?: number;
    settleStableMs?: number;
    maxZoomOutTicks?: number;
  } = {}): Promise<number> {
    let total = 0;
    let zoomOutTicksUsed = 0;
    for (let round = 0; round < maxRounds; round += 1) {
      const wrappers = this.page.locator('[data-testid="show-max-wrapper"]');
      let count = await wrappers.count();

      // Wrappers can be filtered out by virt after the previous round's
      // fitView. Zoom out aggressively to pull them back into viewport.
      while (count === 0 && zoomOutTicksUsed < maxZoomOutTicks) {
        await this.zoomOut(2);
        zoomOutTicksUsed += 2;
        await this.page.waitForTimeout(250);
        count = await wrappers.count();
      }
      if (count === 0) break;

      let clickedThisRound = 0;
      for (let i = 0; i < count; i += 1) {
        const wrapper = wrappers.nth(i);
        try {
          await wrapper.hover({ timeout: 800, force: true });
        } catch {
          continue;
        }
        const showMax = wrapper.locator('[data-testid="show-max"]');
        const showAll = wrapper.locator('[data-testid="show-all"]');
        const showMore = wrapper.locator('[data-testid="show-more"]');
        let target: typeof showMore;
        if ((await showMax.count()) > 0) {
          target = showMax.first();
        } else if ((await showAll.count()) > 0) {
          target = showAll.first();
        } else {
          target = showMore.first();
        }
        try {
          await target.click({ timeout: 1200, force: true });
          total += 1;
          clickedThisRound += 1;
        } catch {
          // Re-layout can detach the button mid-click; try other wrappers.
        }
      }
      if (clickedThisRound === 0) break;

      // Wait for the show-max batches (size 10) to settle, but don't
      // gate on `newCount === lastDomCount` — under virt that's always
      // true regardless of store growth.
      await this.waitForStableNodeCount({
        maxMs: settleMaxMs,
        stableMs: settleStableMs,
      });
    }
    return total;
  }
}
