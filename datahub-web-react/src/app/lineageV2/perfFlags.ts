/**
 * Lineage rendering perf flags.
 *
 * The runtime defaults come from the server-side `featureFlags` block on
 * `AppConfig` — `lineageGraphPerfVirtEnabled` and
 * `lineageGraphPerfOverscanEnabled`. When a server flag is on, the
 * corresponding mode defaults to `'auto'` (apply above the node-count
 * threshold); when off, it defaults to `'off'`.
 *
 * The `?lineagePerf=` URL param and `datahub.lineagePerfFlags` localStorage
 * entry remain as per-session overrides (URL wins over localStorage, which
 * wins over the server default). They exist for diagnostics and customer
 * dogfood; the source of truth for the rollout is the server flag.
 */

export type LineageVirtMode = 'on' | 'off' | 'auto';
export type LineagePerfTriMode = 'on' | 'off' | 'auto';

/** Above this node count, `'auto'` enables `onlyRenderVisibleElements`. */
export const VIRT_AUTO_THRESHOLD = 50;
/** Above this node count, `'auto'` enables the overscan-buffered virt. */
export const OVERSCAN_AUTO_THRESHOLD = 200;

/**
 * Viewport inflation factor when the overscan buffer is active. Conservative
 * because larger values regress pan worst-frame: synthetic_100 measures ~1.5×
 * the strict-viewport pan cost at 0.15.
 */
export const DEFAULT_OVERSCAN_FACTOR = 0.15;

export interface LineagePerfFlags {
    virtMode: LineageVirtMode;
    /** Overscan-buffered virt. Off by default — see {@link DEFAULT_OVERSCAN_FACTOR}. */
    overscanMode: LineagePerfTriMode;
}

/**
 * Server-driven defaults pulled from `AppConfig.featureFlags`. Optional so
 * call sites that pre-date `useAppConfig` wiring keep the legacy hardcoded
 * defaults; production callers should always pass the resolved values.
 */
export interface LineagePerfFeatureFlags {
    virtEnabled?: boolean;
    overscanEnabled?: boolean;
}

const STORAGE_KEY = 'datahub.lineagePerfFlags';
const URL_PARAM = 'lineagePerf';

/**
 * Fallback when no server flags are available — matches the pre-feature-flag
 * behaviour: virt on (auto-thresholded), overscan off.
 */
const HARDCODED_DEFAULTS: LineagePerfFlags = {
    virtMode: 'auto',
    overscanMode: 'off',
};

function resolveDefaults(featureFlags?: LineagePerfFeatureFlags): LineagePerfFlags {
    if (!featureFlags) return HARDCODED_DEFAULTS;
    return {
        virtMode: featureFlags.virtEnabled === false ? 'off' : 'auto',
        overscanMode: featureFlags.overscanEnabled ? 'auto' : 'off',
    };
}

type FlagPatch = Partial<LineagePerfFlags>;

// Bare tokens (no `-on/-off/-auto` suffix) alias to `'on'` for back-compat
// with the original boolean grammar. `defer-minimap` / `no-transition` were
// removed after an N=5 sweep showed p50 within ~5 % of baseline and p95
// spread 3–5× wider; they're accepted as silent no-ops so saved harness
// strings from earlier branches stay functional.
const NO_OP_TOKENS = new Set([
    'defer-minimap',
    'defer-minimap-on',
    'defer-minimap-off',
    'defer-minimap-auto',
    'no-transition',
    'no-transition-on',
    'no-transition-off',
    'no-transition-auto',
]);
const TOKEN_PATCHES: Record<string, FlagPatch> = {
    virt: { virtMode: 'on' },
    'virt-on': { virtMode: 'on' },
    'virt-off': { virtMode: 'off' },
    'virt-auto': { virtMode: 'auto' },
    overscan: { overscanMode: 'on' },
    'overscan-on': { overscanMode: 'on' },
    'overscan-off': { overscanMode: 'off' },
    'overscan-auto': { overscanMode: 'auto' },
};

function parseFlagString(value: string): FlagPatch {
    const out: FlagPatch = {};
    value
        .split(/[,\s+]+/)
        .map((t) => t.trim().toLowerCase())
        .filter(Boolean)
        .forEach((token) => {
            if (NO_OP_TOKENS.has(token)) return;
            const patch = TOKEN_PATCHES[token];
            if (patch) Object.assign(out, patch);
        });
    return out;
}

export function getLineagePerfFlags(featureFlags?: LineagePerfFeatureFlags): LineagePerfFlags {
    const defaults = resolveDefaults(featureFlags);
    if (typeof window === 'undefined') return defaults;
    try {
        const fromUrl = new URLSearchParams(window.location.search).get(URL_PARAM);
        if (fromUrl) return { ...defaults, ...parseFlagString(fromUrl) };
    } catch {
        // window.location can be unavailable in embedded contexts.
    }
    try {
        const fromStorage = window.localStorage.getItem(STORAGE_KEY);
        if (fromStorage) return { ...defaults, ...parseFlagString(fromStorage) };
    } catch {
        // localStorage throws in private-browsing / sandboxed contexts.
    }
    return defaults;
}

/**
 * Resolved against `initialNodes.length` (not a live store sub) so virt is a
 * stable property of the mount — flickering would break screenshot capture
 * and column-edge geometry mid-session.
 */
export function resolveVirtEnabled(mode: LineageVirtMode, nodeCount: number): boolean {
    if (mode === 'on') return true;
    if (mode === 'off') return false;
    return nodeCount > VIRT_AUTO_THRESHOLD;
}

export function resolveTriMode(mode: LineagePerfTriMode, nodeCount: number, autoThreshold: number): boolean {
    if (mode === 'on') return true;
    if (mode === 'off') return false;
    return nodeCount > autoThreshold;
}
