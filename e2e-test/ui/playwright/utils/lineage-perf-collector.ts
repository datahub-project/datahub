/**
 * Lineage rendering perf collector.
 *
 * `installInPage(page)` injects an init script before navigation that sets up
 * `PerformanceObserver` for long tasks, paint, layout-shift, marks, and
 * measures, plus a RAF-driven FPS sampler. The Node-side helpers
 * (`collectRaw`, `start/stopFpsSample`, `summarise`) read the buffers back
 * and roll them into a stable `PerfSummary` shape.
 */

import type { Page } from '@playwright/test';

export interface LongTaskEntry {
  startTime: number;
  duration: number;
}

export interface PaintEntry {
  name: string;
  startTime: number;
}

export interface LayoutShiftEntry {
  startTime: number;
  value: number;
  hadRecentInput: boolean;
}

export interface FpsSample {
  /** Start time of the sample window (DOMHighResTimeStamp). */
  startedAt: number;
  /** Duration of the sample window in ms. */
  durationMs: number;
  /** Total frames painted during the window. */
  frames: number;
  /** Mean frame duration in ms (1000 / fps). */
  meanFrameMs: number;
  /** Worst (longest) frame duration in ms. */
  maxFrameMs: number;
}

export interface ResourceEntry {
  name: string;
  initiatorType: string;
  startTime: number;
  duration: number;
  encodedBodySize: number;
  /** True if the URL contains `/api/v2/graphql` — used to attribute GMS traffic. */
  isGraphql: boolean;
}

export interface RawPerfBuffer {
  longTasks: LongTaskEntry[];
  paint: PaintEntry[];
  layoutShifts: LayoutShiftEntry[];
  marks: Array<{ name: string; startTime: number }>;
  measures: Array<{ name: string; startTime: number; duration: number }>;
  resources: ResourceEntry[];
}

export interface PerfSummary {
  url: string;
  reactFlowNodesInDom: number;
  reactFlowEdgesInDom: number;
  longTaskCount: number;
  longTaskTotalMs: number;
  longTaskMaxMs: number;
  firstPaintMs?: number;
  firstContentfulPaintMs?: number;
  cumulativeLayoutShift: number;
  navigationMs?: number;
  domContentLoadedMs?: number;
  loadEventMs?: number;
  /** All resource entries the browser captured for the page (excludes
   *  data:/blob: which `PerformanceObserver` skips anyway). */
  totalRequests: number;
  /** Subset of `totalRequests` that hit `/api/v2/graphql` — proxy for
   *  GMS round-trips. Lineage fetch + bulk-lineage + expand all flow
   *  through this endpoint, so the count tracks "how chatty did this
   *  variant get?" for a synthetic scale. */
  totalGraphqlRequests: number;
  /** Sum of `encodedBodySize` across resource entries (bytes the
   *  browser actually received over the wire — Service Workers / cache
   *  hits don't inflate it). */
  totalEncodedBodyBytes: number;
}

const INIT_SCRIPT = /* js */ `
(function () {
  if (window.__lineagePerf) return;
  const longTasks = [];
  const paint = [];
  const layoutShifts = [];
  const marks = [];
  const measures = [];
  const resources = [];

  function safeObserve(type, push) {
    try {
      const observer = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) push(entry);
      });
      observer.observe({ type, buffered: true });
    } catch (_err) {
      // Not all entry types are supported on every Chromium version.
    }
  }

  safeObserve('longtask', (e) => longTasks.push({ startTime: e.startTime, duration: e.duration }));
  safeObserve('paint', (e) => paint.push({ name: e.name, startTime: e.startTime }));
  safeObserve('layout-shift', (e) => {
    layoutShifts.push({ startTime: e.startTime, value: e.value, hadRecentInput: e.hadRecentInput });
  });
  safeObserve('mark', (e) => marks.push({ name: e.name, startTime: e.startTime }));
  safeObserve('measure', (e) =>
    measures.push({ name: e.name, startTime: e.startTime, duration: e.duration }),
  );
  // 'resource' covers fetch/xhr/img/script/css; data: and blob: URLs are
  // not reported by the browser, which is what we want — we're counting
  // GMS / asset traffic, not in-memory generated images.
  safeObserve('resource', (e) =>
    resources.push({
      name: e.name,
      initiatorType: e.initiatorType,
      startTime: e.startTime,
      duration: e.duration,
      encodedBodySize: e.encodedBodySize || 0,
      isGraphql: typeof e.name === 'string' && e.name.indexOf('/api/v2/graphql') !== -1,
    }),
  );

  // FPS sampler: accumulate inter-frame deltas; the Node side computes stats.
  let fpsState = null;
  function fpsTick(ts) {
    if (!fpsState) return;
    if (fpsState.lastTs !== null) {
      const delta = ts - fpsState.lastTs;
      fpsState.deltas.push(delta);
    }
    fpsState.lastTs = ts;
    fpsState.handle = requestAnimationFrame(fpsTick);
  }

  window.__lineagePerf = { longTasks, paint, layoutShifts, marks, measures, resources };

  window.__lineagePerfStartFps = function () {
    if (fpsState) return;
    fpsState = { startedAt: performance.now(), lastTs: null, deltas: [], handle: null };
    fpsState.handle = requestAnimationFrame(fpsTick);
  };

  window.__lineagePerfStopFps = function () {
    if (!fpsState) return null;
    const ended = performance.now();
    if (fpsState.handle !== null) cancelAnimationFrame(fpsState.handle);
    const result = {
      startedAt: fpsState.startedAt,
      durationMs: ended - fpsState.startedAt,
      deltas: fpsState.deltas.slice(),
    };
    fpsState = null;
    return result;
  };
})();
`;

export async function installInPage(page: Page): Promise<void> {
  await page.addInitScript({ content: INIT_SCRIPT });
}

export async function collectRaw(page: Page): Promise<RawPerfBuffer> {
  return page.evaluate(() => {
    type Window = typeof globalThis & { __lineagePerf?: RawPerfBuffer };
    const w = window as Window;
    return (
      w.__lineagePerf ?? {
        longTasks: [],
        paint: [],
        layoutShifts: [],
        marks: [],
        measures: [],
        resources: [],
      }
    );
  });
}

export async function startFpsSample(page: Page): Promise<void> {
  await page.evaluate(() => {
    type W = typeof globalThis & { __lineagePerfStartFps?: () => void };
    (window as W).__lineagePerfStartFps?.();
  });
}

export async function stopFpsSample(page: Page): Promise<FpsSample | null> {
  const raw = await page.evaluate(() => {
    type W = typeof globalThis & {
      __lineagePerfStopFps?: () => { startedAt: number; durationMs: number; deltas: number[] } | null;
    };
    return (window as W).__lineagePerfStopFps?.() ?? null;
  });
  if (!raw) return null;
  const frames = raw.deltas.length;
  const meanFrameMs = frames > 0 ? raw.deltas.reduce((s: number, d: number) => s + d, 0) / frames : 0;
  const maxFrameMs = frames > 0 ? Math.max(...raw.deltas) : 0;
  return {
    startedAt: raw.startedAt,
    durationMs: raw.durationMs,
    frames,
    meanFrameMs,
    maxFrameMs,
  };
}

export async function countReactFlowDomNodes(page: Page): Promise<{ nodes: number; edges: number }> {
  return page.evaluate(() => ({
    nodes: document.querySelectorAll('.react-flow__node').length,
    edges: document.querySelectorAll('.react-flow__edge').length,
  }));
}

export async function summarise(page: Page): Promise<PerfSummary> {
  const raw = await collectRaw(page);
  const dom = await countReactFlowDomNodes(page);
  const nav = await page.evaluate(() => {
    const entry = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined;
    if (!entry) return null;
    return {
      url: location.href,
      navigationMs: entry.responseEnd - entry.startTime,
      domContentLoadedMs: entry.domContentLoadedEventEnd - entry.startTime,
      loadEventMs: entry.loadEventEnd - entry.startTime,
    };
  });

  const firstPaint = raw.paint.find((p) => p.name === 'first-paint')?.startTime;
  const firstContentfulPaint = raw.paint.find((p) => p.name === 'first-contentful-paint')?.startTime;
  const cls = raw.layoutShifts.reduce((s, l) => (l.hadRecentInput ? s : s + l.value), 0);
  const longTaskTotalMs = raw.longTasks.reduce((s, l) => s + l.duration, 0);
  const longTaskMaxMs = raw.longTasks.reduce((m, l) => Math.max(m, l.duration), 0);
  const totalGraphqlRequests = raw.resources.reduce((s, r) => s + (r.isGraphql ? 1 : 0), 0);
  const totalEncodedBodyBytes = raw.resources.reduce((s, r) => s + r.encodedBodySize, 0);

  return {
    url: nav?.url ?? '',
    reactFlowNodesInDom: dom.nodes,
    reactFlowEdgesInDom: dom.edges,
    longTaskCount: raw.longTasks.length,
    longTaskTotalMs,
    longTaskMaxMs,
    firstPaintMs: firstPaint,
    firstContentfulPaintMs: firstContentfulPaint,
    cumulativeLayoutShift: cls,
    navigationMs: nav?.navigationMs,
    domContentLoadedMs: nav?.domContentLoadedMs,
    loadEventMs: nav?.loadEventMs,
    totalRequests: raw.resources.length,
    totalGraphqlRequests,
    totalEncodedBodyBytes,
  };
}
