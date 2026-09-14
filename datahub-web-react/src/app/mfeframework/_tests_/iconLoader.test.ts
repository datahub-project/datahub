import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { makeLoadIcon } from '@app/mfeframework/iconLoader';

const MockIcon = () => null;

describe('makeLoadIcon', () => {
    let mocks: Record<string, ReturnType<typeof vi.fn>>;

    beforeEach(() => {
        mocks = {
            './lazy-icons/Acorn.ts': vi.fn(async () => ({ Acorn: MockIcon })),
            './lazy-icons/Globe.ts': vi.fn(async () => ({ Globe: MockIcon })),
            // Simulates a stub whose named export was eliminated (e.g. by a misconfigured build).
            './lazy-icons/Stripped.ts': vi.fn(async () => ({ WrongName: MockIcon })),
        };
    });

    it('returns the correct named export for a known icon', async () => {
        const result = await makeLoadIcon(mocks)('Acorn');
        expect(result.default).toBe(MockIcon);
    });

    it('falls back to AppWindow for an icon not in the glob map', async () => {
        const result = await makeLoadIcon(mocks)('NotAReal Icon');
        expect(result.default).toBe(AppWindow);
    });

    it('falls back to AppWindow when the stub export name does not match — tree-shaking guard', async () => {
        // If a build tool strips or renames the export, mod[name] is undefined.
        // The fallback prevents passing undefined to React as a component.
        const result = await makeLoadIcon(mocks)('Stripped');
        expect(result.default).toBe(AppWindow);
    });

    it('logs a warning when the icon is not found', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        await makeLoadIcon(mocks)('Missing');
        expect(warn).toHaveBeenCalledWith(expect.stringContaining('"Missing"'));
        warn.mockRestore();
    });

    it('fetches via ./lazy-icons/<name>.ts so each icon is its own async chunk', async () => {
        await makeLoadIcon(mocks)('Globe');
        expect(mocks['./lazy-icons/Globe.ts']).toHaveBeenCalledOnce();
    });
});
