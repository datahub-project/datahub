import { afterEach, describe, expect, it, vi } from 'vitest';

import {
    loadPhosphorIcons,
    resetPhosphorIconsCacheForTests,
} from '@app/entityV2/shared/containers/profile/header/IconPicker/loadPhosphorIcons';

describe('loadPhosphorIcons', () => {
    afterEach(() => {
        resetPhosphorIconsCacheForTests();
    });

    it('reuses one in-flight promise across callers', async () => {
        const importer = vi.fn().mockResolvedValue({ House: () => null });
        resetPhosphorIconsCacheForTests(importer);

        const first = loadPhosphorIcons();
        const second = loadPhosphorIcons();
        expect(second).toBe(first);
        await expect(first).resolves.toEqual({ House: expect.any(Function) });
        expect(importer).toHaveBeenCalledTimes(1);
    });

    it('clears the cached promise after rejection so a later call can retry', async () => {
        const importer = vi
            .fn()
            .mockRejectedValueOnce(new Error('chunk failed'))
            .mockResolvedValueOnce({ House: () => null });
        resetPhosphorIconsCacheForTests(importer);

        await expect(loadPhosphorIcons()).rejects.toThrow('chunk failed');
        await expect(loadPhosphorIcons()).resolves.toEqual({ House: expect.any(Function) });
        expect(importer).toHaveBeenCalledTimes(2);
    });
});
