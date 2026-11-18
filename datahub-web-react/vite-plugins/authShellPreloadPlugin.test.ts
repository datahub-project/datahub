import { describe, expect, it } from 'vitest';

// eslint-disable-next-line import-alias/import-alias -- plugin tests sit next to the plugin, outside src aliases
import { collectAuthShellPreloadFiles, renderAuthShellPreloadScript } from './authShellPreloadPlugin';

describe('collectAuthShellPreloadFiles', () => {
    const bundle = {
        'assets/index.js': {
            type: 'chunk',
            fileName: 'assets/index.js',
            isEntry: true,
            facadeModuleId: '/src/index.tsx',
            imports: ['assets/vendor.js'],
            modules: {},
        },
        'assets/AuthenticatedApp.js': {
            type: 'chunk',
            fileName: 'assets/AuthenticatedApp.js',
            isEntry: false,
            facadeModuleId: '/src/app/AuthenticatedApp.tsx',
            imports: ['assets/AppProviders.js', 'assets/index.js'],
            modules: { '/src/app/AuthenticatedApp.tsx': {} },
        },
        'assets/AppProviders.js': {
            type: 'chunk',
            fileName: 'assets/AppProviders.js',
            isEntry: false,
            facadeModuleId: '/src/app/AppProviders.tsx',
            imports: ['assets/index.js'],
            dynamicImports: ['assets/HomePage.js'],
            modules: {},
        },
        'assets/HomePage.js': {
            type: 'chunk',
            fileName: 'assets/HomePage.js',
            isEntry: false,
            facadeModuleId: '/src/app/homeV2/HomePage.tsx',
            imports: [],
            modules: {},
        },
        'assets/index.css': { type: 'asset', fileName: 'assets/index.css' },
    };

    it('preloads the auth shell static graph and leaves the entry and route chunks out', () => {
        expect(collectAuthShellPreloadFiles(bundle)).toEqual(['assets/AuthenticatedApp.js', 'assets/AppProviders.js']);
    });

    it('returns nothing when the shell chunk is absent', () => {
        expect(collectAuthShellPreloadFiles({ 'assets/index.js': bundle['assets/index.js'] })).toEqual([]);
    });
});

describe('renderAuthShellPreloadScript', () => {
    it('injects modulepreloads only when the actor cookie has a value', () => {
        const script = renderAuthShellPreloadScript(['assets/AppProviders.js']);
        expect(script).toContain('part.indexOf("actor=")===0&&part.length>6');
        expect(script).toContain('link.rel="modulepreload"');
        expect(script).toContain('["assets/AppProviders.js"]');
        expect(renderAuthShellPreloadScript([])).toBe('');
    });
});
