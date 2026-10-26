import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { installChunkReloadHandler } from '@app/sharedV2/chunkReload';

function firePreloadError(): boolean {
    const event = new Event('vite:preloadError', { cancelable: true });
    window.dispatchEvent(event);
    return event.defaultPrevented;
}

describe('installChunkReloadHandler', () => {
    let reload: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        window.sessionStorage.clear();
        reload = vi.fn();
        Object.defineProperty(window, 'location', {
            configurable: true,
            value: { ...window.location, reload },
        });
        installChunkReloadHandler();
    });

    afterEach(() => {
        vi.restoreAllMocks();
        window.sessionStorage.clear();
    });

    it('reloads once so a stale chunk picks up the new manifest', () => {
        expect(firePreloadError()).toBe(true);
        expect(reload).toHaveBeenCalledTimes(1);
    });

    it('does not reload again within the cooldown, leaving the error to the boundary', () => {
        firePreloadError();
        reload.mockClear();

        expect(firePreloadError()).toBe(false);
        expect(reload).not.toHaveBeenCalled();
    });

    it('does not reload when session storage is blocked', () => {
        vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
            throw new Error('blocked');
        });
        vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
            throw new Error('blocked');
        });

        expect(firePreloadError()).toBe(false);
        expect(reload).not.toHaveBeenCalled();
    });

    it('reloads again once the cooldown has elapsed', () => {
        firePreloadError();
        reload.mockClear();

        vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 31_000);

        expect(firePreloadError()).toBe(true);
        expect(reload).toHaveBeenCalledTimes(1);
    });
});
