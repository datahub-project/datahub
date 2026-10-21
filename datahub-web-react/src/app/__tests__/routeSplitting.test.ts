import fs from 'fs';
import path from 'path';
import { describe, expect, it } from 'vitest';

const WEB_REACT_ROOT = path.resolve(__dirname, '../../..');

function read(relPath: string): string {
    return fs.readFileSync(path.join(WEB_REACT_ROOT, relPath), 'utf8');
}

const STATIC_PAGE_IMPORT =
    /^import\s+(?:type\s+)?(?:[\w*\s{},]+)\s+from\s+['"]@app\/(?:searchV2\/SearchPage|entityV2\/EntityPage|chat\/ChatPage|settingsV2\/SettingsPage|homeV2\/HomePage|homeV3\/HomePage|SearchRoutes|AuthenticatedApp)['"]/m;

describe('route-level lazy loading', () => {
    it('does not pin src/ to a named source chunk', () => {
        const viteConfig = read('vite.config.ts');
        expect(viteConfig).not.toMatch(/return\s+['"]source['"]/);
        expect(viteConfig).not.toMatch(/return\s+['"]phosphor-vendor['"]/);
        expect(viteConfig).not.toMatch(/manualChunks\s*\(/);
        expect(viteConfig).toMatch(/No manualChunks/);
    });

    it('lazy-loads the authenticated shell from Routes', () => {
        const src = read('src/app/Routes.tsx');
        expect(src).toMatch(/lazyDefault\(\(\) => import\('@app\/AuthenticatedApp'\)\)/);
        expect(src).not.toMatch(/^import\s+AuthenticatedApp/m);
        expect(src).not.toMatch(/^import\s+AppProviders/m);
    });

    it('lazy-loads homepage and SearchRoutes from ProtectedRoutes', () => {
        const src = read('src/app/ProtectedRoutes.tsx');
        expect(src).toMatch(/lazyNamed\(\(\) => import\('@app\/SearchRoutes'\),\s*'SearchRoutes'\)/);
        expect(src).toMatch(/lazyNamed\(\(\) => import\('@app\/homeV3\/HomePage'/);
        expect(src).not.toMatch(STATIC_PAGE_IMPORT);
    });

    it('lazy-loads page modules from SearchRoutes rather than importing them statically', () => {
        const src = read('src/app/SearchRoutes.tsx');
        expect(src).toMatch(/lazyNamed\(\(\) => import\('@app\/searchV2\/SearchPage'/);
        expect(src).toMatch(/lazyNamed\(\(\) => import\('@app\/entityV2\/EntityPage'/);
        expect(src).toMatch(/lazyNamed\(\(\) => import\('@app\/settingsV2\/SettingsPage'/);
        expect(src).not.toMatch(/^import\s+\{[^}]*SearchPage[^}]*\}\s+from\s+['"]@app\/searchV2\/SearchPage['"]/m);
        expect(src).not.toMatch(/^import\s+\{[^}]*EntityPage[^}]*\}\s+from\s+['"]@app\/entityV2\/EntityPage['"]/m);
        expect(src).not.toMatch(STATIC_PAGE_IMPORT);
    });
});
