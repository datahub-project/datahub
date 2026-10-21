import { describe, expect, it } from 'vitest';

// eslint-disable-next-line import-alias/import-alias -- plugin tests sit next to the plugin, outside src aliases
import {
    collectAuthShellPreloadFiles,
    collectRoutePreloadGroups,
    renderAuthShellPreloadScript,
    renderRoutePreloadScript,
    routePreloadKey,
} from './authShellPreloadPlugin';

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
        expect(script).toContain('"shell":["assets/AppProviders.js"]');
        expect(renderAuthShellPreloadScript([])).toBe('');
    });
});

describe('route preload groups', () => {
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
            imports: ['assets/providers.js'],
            modules: {},
        },
        'assets/providers.js': {
            type: 'chunk',
            fileName: 'assets/providers.js',
            isEntry: false,
            facadeModuleId: '/src/app/AppProviders.tsx',
            imports: [],
            modules: {},
        },
        'assets/HomePage.js': {
            type: 'chunk',
            fileName: 'assets/HomePage.js',
            isEntry: false,
            facadeModuleId: '/src/app/homeV3/HomePage.tsx',
            imports: ['assets/providers.js', 'assets/home-shared.js'],
            modules: {},
        },
        'assets/home-shared.js': {
            type: 'chunk',
            fileName: 'assets/home-shared.js',
            isEntry: false,
            facadeModuleId: '/src/app/homeV3/Template.tsx',
            imports: [],
            modules: {},
        },
        'assets/SearchRoutes.js': {
            type: 'chunk',
            fileName: 'assets/SearchRoutes.js',
            isEntry: false,
            facadeModuleId: '/src/app/SearchRoutes.tsx',
            imports: ['assets/providers.js'],
            modules: {},
        },
        'assets/SearchPage.js': {
            type: 'chunk',
            fileName: 'assets/SearchPage.js',
            isEntry: false,
            facadeModuleId: '/src/app/searchV2/SearchPage.tsx',
            imports: ['assets/search-shared.js'],
            modules: {},
        },
        'assets/search-shared.js': {
            type: 'chunk',
            fileName: 'assets/search-shared.js',
            isEntry: false,
            facadeModuleId: '/src/app/searchV2/SearchBar.tsx',
            imports: [],
            modules: {},
        },
        'assets/EntityPage.js': {
            type: 'chunk',
            fileName: 'assets/EntityPage.js',
            isEntry: false,
            facadeModuleId: '/src/app/entityV2/EntityPage.tsx',
            imports: [],
            modules: {},
        },
        'assets/EntityProfile.js': {
            type: 'chunk',
            fileName: 'assets/EntityProfile.js',
            isEntry: false,
            facadeModuleId: '/src/app/entityV2/shared/containers/profile/EntityProfile.tsx',
            imports: ['assets/profile-shared.js'],
            modules: {},
        },
        'assets/profile-shared.js': {
            type: 'chunk',
            fileName: 'assets/profile-shared.js',
            isEntry: false,
            facadeModuleId: '/src/app/entityV2/shared/containers/profile/header/EntityTabs.tsx',
            imports: [],
            modules: {},
        },
        'assets/SchemaTab.js': {
            type: 'chunk',
            fileName: 'assets/SchemaTab.js',
            isEntry: false,
            facadeModuleId: '/src/app/entityV2/shared/tabs/Dataset/Schema/SchemaTab.tsx',
            imports: [],
            modules: {},
        },
    };

    it('preloads the shell everywhere and adds the current route graph', () => {
        const groups = collectRoutePreloadGroups(bundle);
        expect(groups.shell).toEqual(['assets/AuthenticatedApp.js', 'assets/providers.js']);
        expect(groups.home).toEqual([
            'assets/AuthenticatedApp.js',
            'assets/providers.js',
            'assets/HomePage.js',
            'assets/home-shared.js',
        ]);
        expect(groups.search).toEqual([
            'assets/AuthenticatedApp.js',
            'assets/providers.js',
            'assets/SearchRoutes.js',
            'assets/SearchPage.js',
            'assets/search-shared.js',
        ]);
        expect(groups.dataset).toEqual([
            'assets/AuthenticatedApp.js',
            'assets/providers.js',
            'assets/SearchRoutes.js',
            'assets/EntityPage.js',
            'assets/EntityProfile.js',
            'assets/profile-shared.js',
        ]);
        expect(groups.dataset).not.toContain('assets/SchemaTab.js');
        expect(groups.home).not.toContain('assets/SearchPage.js');
    });

    it('picks the route from the pathname after the base href', () => {
        expect(routePreloadKey('/', '/')).toBe('home');
        expect(routePreloadKey('/search', '/')).toBe('search');
        expect(routePreloadKey('/search/', '/')).toBe('search');
        expect(routePreloadKey('/dataset/urn:li:dataset:x', '/')).toBe('dataset');
        expect(routePreloadKey('/settings', '/')).toBe('shell');
        expect(routePreloadKey('/datahub/search', '/datahub/')).toBe('search');
        expect(routePreloadKey('/login', '@basePath')).toBe('shell');
    });

    it('embeds each route list and skips the preload until the actor cookie is set', () => {
        const groups = collectRoutePreloadGroups(bundle);
        const script = renderRoutePreloadScript(groups);
        expect(script).toContain(JSON.stringify(groups));
        expect(script).toContain('if(!actor)return');
        expect(script).toContain('path==="/search"||path.indexOf("/search/")===0');
        expect(script).toContain('path==="/dataset"||path.indexOf("/dataset/")===0');
        expect(script).not.toContain('SchemaTab');
    });
});
