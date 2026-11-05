const LAST_RELOAD_KEY = 'datahub.chunkReloadAt';
const RELOAD_COOLDOWN_MS = 30_000;

function readLastReload(): number {
    try {
        return Number(window.sessionStorage.getItem(LAST_RELOAD_KEY)) || 0;
    } catch {
        // Storage can throw when cookies/site data are blocked.
        return 0;
    }
}

function recordReload(at: number): void {
    try {
        window.sessionStorage.setItem(LAST_RELOAD_KEY, String(at));
    } catch {
        // Without storage we cannot rate-limit, so skip the reload entirely rather
        // than risk an infinite loop.
    }
}

/**
 * Recover from stale chunk URLs after a deploy.
 *
 * Route-level splitting puts a dynamic import on the main navigation path, so a
 * session holds hashed chunk URLs from the build it loaded. A deploy replaces those files and
 * the next navigation rejects with a preload error, stranding the user on the error page until
 * they refresh by hand. Reload once so the new manifest is picked up.
 *
 * The cooldown matters: if the chunk is genuinely missing rather than merely stale, reloading
 * would loop. After the cooldown the event is left unhandled so the route ErrorBoundary renders
 * its fallback instead.
 */
export function installChunkReloadHandler(): void {
    window.addEventListener('vite:preloadError', (event) => {
        const now = Date.now();
        if (now - readLastReload() < RELOAD_COOLDOWN_MS) {
            return;
        }
        const before = readLastReload();
        recordReload(now);
        if (readLastReload() === before) {
            return;
        }
        event.preventDefault();
        window.location.reload();
    });
}
