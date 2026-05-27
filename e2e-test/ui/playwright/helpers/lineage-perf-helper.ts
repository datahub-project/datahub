/**
 * LineagePerfRecorder — `measure(label, fn)` around UI interactions.
 *
 * Records wall time, long-task delta (only tasks fired during the action),
 * and optional FPS sample (`measureWithFps` for pan/zoom). Output written
 * to `e2e-test/ui/playwright/test-results/lineage-perf.json`.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import path from 'path';
import type { Page } from '@playwright/test';

import {
  collectRaw,
  installInPage,
  startFpsSample,
  stopFpsSample,
  summarise,
  type FpsSample,
  type LongTaskEntry,
  type PerfSummary,
} from '../utils/lineage-perf-collector';

export interface MeasuredAction {
  label: string;
  wallMs: number;
  longTasksDuring: LongTaskEntry[];
  longTaskTotalMs: number;
  longTaskMaxMs: number;
  fpsDuringAction?: FpsSample;
  /** True when a `timeoutMs` cap was hit; `wallMs` reflects the cap, not completion. */
  timedOut?: boolean;
  /** Total resource entries (fetch/xhr/script/css/img/...) that fired
   *  during the action. */
  requestsDuring: number;
  /** Subset of `requestsDuring` that hit `/api/v2/graphql` — useful for
   *  spotting variants that quietly fan out more GMS calls. */
  graphqlRequestsDuring: number;
  /** Sum of `encodedBodySize` across requests fired during the action. */
  requestBytesDuring: number;
}

export interface ScenarioRecording {
  scenario: string;
  /** Flag combination active during the recording. Consumers slice by `(scenario, variant)`. */
  variant: string;
  /**
   * 1-indexed run number when `LINEAGE_PERF_REPEAT > 1`. Aggregators key
   * (scenario, variant, action) → samples by `iteration` to compute
   * percentile bands; absent / `1` for single-shot runs.
   */
  iteration?: number;
  url: string;
  actions: MeasuredAction[];
  summary: PerfSummary;
}

export class LineagePerfRecorder {
  private readonly page: Page;
  private readonly actions: MeasuredAction[] = [];

  constructor(page: Page) {
    this.page = page;
  }

  /** Must run BEFORE any navigation so PerformanceObserver catches the first frame. */
  async install(): Promise<void> {
    await installInPage(this.page);
  }

  /**
   * Bake any settle wait into `fn` itself — long tasks fired after `fn`
   * resolves are not attributed. Pass `timeoutMs` to cap with `Promise.race`;
   * a timed-out action records `timedOut: true` and `wallMs = cap`.
   */
  async measure<T>(
    label: string,
    fn: () => Promise<T>,
    { timeoutMs }: { timeoutMs?: number } = {},
  ): Promise<MeasuredAction> {
    const longTasksBefore = await this.longTaskCount();
    const resourcesBefore = await this.resourceCount();
    const start = Date.now();
    const timedOut = await this.runWithTimeout(fn, timeoutMs);
    const wallMs = Date.now() - start;
    const tasks = await this.longTasksSince(longTasksBefore);
    const resourceDelta = await this.resourcesSince(resourcesBefore);
    const action: MeasuredAction = {
      label,
      wallMs,
      longTasksDuring: tasks,
      longTaskTotalMs: tasks.reduce((s, t) => s + t.duration, 0),
      longTaskMaxMs: tasks.reduce((m, t) => Math.max(m, t.duration), 0),
      requestsDuring: resourceDelta.total,
      graphqlRequestsDuring: resourceDelta.graphql,
      requestBytesDuring: resourceDelta.bytes,
      ...(timedOut ? { timedOut: true } : {}),
    };
    this.actions.push(action);
    return action;
  }

  /** Like {@link measure}, plus an FPS sample. Use for sustained animation. */
  async measureWithFps<T>(
    label: string,
    fn: () => Promise<T>,
    { timeoutMs }: { timeoutMs?: number } = {},
  ): Promise<MeasuredAction> {
    const longTasksBefore = await this.longTaskCount();
    const resourcesBefore = await this.resourceCount();
    await startFpsSample(this.page);
    const start = Date.now();
    const timedOut = await this.runWithTimeout(fn, timeoutMs);
    const wallMs = Date.now() - start;
    const fps = await stopFpsSample(this.page);
    const tasks = await this.longTasksSince(longTasksBefore);
    const resourceDelta = await this.resourcesSince(resourcesBefore);
    const action: MeasuredAction = {
      label,
      wallMs,
      longTasksDuring: tasks,
      longTaskTotalMs: tasks.reduce((s, t) => s + t.duration, 0),
      longTaskMaxMs: tasks.reduce((m, t) => Math.max(m, t.duration), 0),
      fpsDuringAction: fps ?? undefined,
      requestsDuring: resourceDelta.total,
      graphqlRequestsDuring: resourceDelta.graphql,
      requestBytesDuring: resourceDelta.bytes,
      ...(timedOut ? { timedOut: true } : {}),
    };
    this.actions.push(action);
    return action;
  }

  private async runWithTimeout<T>(fn: () => Promise<T>, timeoutMs?: number): Promise<boolean> {
    if (!timeoutMs) {
      try {
        await fn();
        return false;
      } catch {
        // Swallow — long-task / wall data is still useful even if the gesture failed.
        return false;
      }
    }
    let timer: ReturnType<typeof setTimeout> | null = null;
    const timeoutPromise = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => resolve('timeout'), timeoutMs);
    });
    try {
      const result = await Promise.race<'timeout' | T>([fn(), timeoutPromise]);
      return result === 'timeout';
    } catch {
      return false;
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  /** Snapshot the final per-page summary (node/edge count, paint, CLS, …). */
  async summarise(): Promise<PerfSummary> {
    return summarise(this.page);
  }

  /** All measured actions in capture order. */
  recordedActions(): MeasuredAction[] {
    return this.actions.slice();
  }

  private async longTaskCount(): Promise<number> {
    const raw = await collectRaw(this.page);
    return raw.longTasks.length;
  }

  private async longTasksSince(previousCount: number): Promise<LongTaskEntry[]> {
    const raw = await collectRaw(this.page);
    return raw.longTasks.slice(previousCount);
  }

  private async resourceCount(): Promise<number> {
    const raw = await collectRaw(this.page);
    return raw.resources.length;
  }

  private async resourcesSince(previousCount: number): Promise<{ total: number; graphql: number; bytes: number }> {
    const raw = await collectRaw(this.page);
    const slice = raw.resources.slice(previousCount);
    return {
      total: slice.length,
      graphql: slice.reduce((s, r) => s + (r.isGraphql ? 1 : 0), 0),
      bytes: slice.reduce((s, r) => s + r.encodedBodySize, 0),
    };
  }
}

/**
 * Append a recording to the perf results file. Reads + rewrites the whole
 * array each call (inefficient, but Playwright worker respawns wipe module
 * state so we can't accumulate in memory). Call {@link resetRecordingFile}
 * once per worker boot to clear stale results.
 */
export function emitRecording(recording: ScenarioRecording, outFile = 'lineage-perf.json'): void {
  const outDir = path.resolve(__dirname, '../test-results');
  try {
    mkdirSync(outDir, { recursive: true });
  } catch {
    // Best-effort.
  }
  const out = path.join(outDir, outFile);
  let existing: ScenarioRecording[] = [];
  if (existsSync(out)) {
    try {
      const raw = readFileSync(out, 'utf8');
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) existing = parsed as ScenarioRecording[];
    } catch {
      // Treat unreadable file as empty.
    }
  }
  existing.push(recording);
  writeFileSync(out, JSON.stringify(existing, null, 2));
}

export function resetRecordingFile(outFile = 'lineage-perf.json'): void {
  const outDir = path.resolve(__dirname, '../test-results');
  try {
    mkdirSync(outDir, { recursive: true });
  } catch {
    // Best-effort.
  }
  writeFileSync(path.join(outDir, outFile), '[]');
}
