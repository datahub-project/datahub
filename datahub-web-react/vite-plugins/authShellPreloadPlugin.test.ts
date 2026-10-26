import { afterEach, describe, expect, it, vi } from 'vitest';
import { runInNewContext } from 'vm';

// eslint-disable-next-line import-alias/import-alias -- plugin tests sit next to the plugin, outside src aliases
import {
    PreloadChunk,
    ROUTE_PRELOADS,
    collectAuthShellPreloadFiles,
    collectRoutePreloadFiles,
    renderAuthShellPreloadScript,
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
        expect(script).toContain('["assets/AppProviders.js"]');
        expect(renderAuthShellPreloadScript([])).toBe('');
    });
});

describe('collectRoutePreloadFiles', () => {
    const chunk = (fileName: string, facadeModuleId: string | null, imports: string[] = []): PreloadChunk => ({
        type: 'chunk',
        fileName,
        isEntry: false,
        facadeModuleId,
        imports,
        modules: facadeModuleId ? { [facadeModuleId]: {} } : {},
    });
    const bundle: Record<string, PreloadChunk> = {
        'assets/index.js': {
            type: 'chunk',
            fileName: 'assets/index.js',
            isEntry: true,
            facadeModuleId: '/src/index.tsx',
            imports: [],
            modules: {},
        },
        'assets/AuthenticatedApp.js': chunk('assets/AuthenticatedApp.js', '/src/app/AuthenticatedApp.tsx', [
            'assets/AppProviders.js',
            'assets/index.js',
        ]),
        'assets/AppProviders.js': chunk('assets/AppProviders.js', '/src/app/AppProviders.tsx', ['assets/index.js']),
        'assets/HomePage.js': chunk('assets/HomePage.js', '/src/app/homeV3/HomePage.tsx', [
            'assets/HomeModules.js',
            'assets/AppProviders.js',
            'assets/index.js',
        ]),
        'assets/HomeModules.js': chunk('assets/HomeModules.js', null, ['assets/shared.js']),
        'assets/shared.js': chunk('assets/shared.js', null),
        'assets/SearchRoutes.js': chunk('assets/SearchRoutes.js', '/src/app/SearchRoutes.tsx', [
            'assets/SearchablePage.js',
            'assets/AppProviders.js',
        ]),
        'assets/SearchablePage.js': chunk('assets/SearchablePage.js', null, ['assets/shared.js']),
        'assets/SearchPage.js': chunk('assets/SearchPage.js', '/src/app/searchV2/SearchPage.tsx', [
            'assets/shared.js',
            'assets/SearchResults.js',
        ]),
        'assets/SearchResults.js': chunk('assets/SearchResults.js', null),
        'assets/EntityPage.js': chunk('assets/EntityPage.js', '/src/app/entityV2/EntityPage.tsx', ['assets/shared.js']),
        // Not a facade: EntityProfile shares a chunk with another module.
        'assets/profile.js': {
            ...chunk('assets/profile.js', null),
            modules: {
                '/src/app/entityV2/shared/containers/profile/EntityProfile.tsx': {},
                '/src/app/entityV2/shared/containers/profile/header/EntityHeader.tsx': {},
            },
        },
    };
    const shellFiles = collectAuthShellPreloadFiles(bundle);

    it('lists page chunks first, then their static imports, without the shell or entry', () => {
        const routes = collectRoutePreloadFiles(bundle, shellFiles);
        expect(shellFiles).toEqual(['assets/AuthenticatedApp.js', 'assets/AppProviders.js']);
        expect(routes.map((route) => [String(route.path), route.files])).toEqual([
            [String(/^\/$/), ['assets/HomePage.js', 'assets/HomeModules.js', 'assets/shared.js']],
            [
                String(/^\/search(?:\/|$)/),
                [
                    'assets/SearchRoutes.js',
                    'assets/SearchPage.js',
                    'assets/SearchablePage.js',
                    'assets/shared.js',
                    'assets/SearchResults.js',
                ],
            ],
            [
                String(/^\/(?:dataset|chart|dashboard|container|pipelines|tasks)\//),
                [
                    'assets/SearchRoutes.js',
                    'assets/EntityPage.js',
                    'assets/profile.js',
                    'assets/SearchablePage.js',
                    'assets/shared.js',
                ],
            ],
        ]);
    });

    it('skips a route whose module is missing and reports it', () => {
        const onMissing = vi.fn();
        const routes = collectRoutePreloadFiles(bundle, shellFiles, ROUTE_PRELOADS, onMissing);
        // homeV2/HomePage is not in this bundle, so only the V3 home entry is left.
        expect(routes.some((route) => route.storage)).toBe(false);
        expect(onMissing).toHaveBeenCalledTimes(1);
        expect(String(onMissing.mock.calls[0][0])).toContain('homeV2');

        const withoutSearchRoutes = Object.fromEntries(
            Object.entries(bundle).filter(([fileName]) => fileName !== 'assets/SearchRoutes.js'),
        );
        expect(collectRoutePreloadFiles(withoutSearchRoutes, shellFiles).map((route) => String(route.path))).toEqual([
            String(/^\/$/),
        ]);
    });

    it('skips a route whose chunks are all in the shell', () => {
        const target = { path: /^\/x$/, modules: [/(?:^|\/)app\/AppProviders\.tsx$/] };
        expect(collectRoutePreloadFiles(bundle, shellFiles, [target])).toEqual([]);
    });

    it('keeps the storage condition of a route', () => {
        const target = {
            path: /^\/$/,
            modules: [/(?:^|\/)app\/homeV3\/HomePage\.tsx$/],
            storage: { key: 'flag', value: 'false' },
        };
        expect(collectRoutePreloadFiles(bundle, shellFiles, [target])[0].storage).toEqual({
            key: 'flag',
            value: 'false',
        });
    });
});

describe('renderAuthShellPreloadScript with routes', () => {
    const shell = ['assets/AuthenticatedApp.js'];
    const routes = [
        { path: /^\/$/, files: ['assets/HomeV2.js', 'assets/shared.js'], storage: { key: 'redesign', value: 'false' } },
        { path: /^\/$/, files: ['assets/HomeV3.js', 'assets/shared.js'] },
        { path: /^\/search(?:\/|$)/, files: ['assets/SearchRoutes.js', 'assets/SearchPage.js'] },
    ];

    // setupTests replaces window.location with a plain object, so the script gets its own.
    const run = (pathname: string, cookie = 'actor=urn:li:corpuser:datahub') => {
        document.cookie = cookie;
        const script = renderAuthShellPreloadScript(shell, routes);
        runInNewContext(script.replace(/^<script>/, '').replace(/<\/script>$/, ''), {
            document,
            localStorage,
            location: { pathname },
        });
        return Array.from(document.head.querySelectorAll('link[rel="modulepreload"]')).map((link) =>
            link.getAttribute('href'),
        );
    };

    afterEach(() => {
        document.head.querySelectorAll('link[rel="modulepreload"], base').forEach((node) => node.remove());
        document.cookie = 'actor=; expires=Thu, 01 Jan 1970 00:00:00 GMT';
        localStorage.clear();
    });

    it('lists each file once and refers to it by index', () => {
        const script = renderAuthShellPreloadScript(shell, routes);
        expect(script.match(/assets\/shared\.js/g)).toHaveLength(1);
        expect(script).toContain('[/^\\/search(?:\\/|$)/,[4,5]]');
    });

    it('adds the shell, then the matching route with its main chunk first', () => {
        expect(run('/search/dataset')).toEqual([
            'assets/AuthenticatedApp.js',
            'assets/SearchRoutes.js',
            'assets/SearchPage.js',
        ]);
    });

    it('only adds the shell when no route matches', () => {
        expect(run('/settings')).toEqual(['assets/AuthenticatedApp.js']);
        document.head.querySelectorAll('link[rel="modulepreload"]').forEach((node) => node.remove());
        expect(run('/searchable')).toEqual(['assets/AuthenticatedApp.js']);
    });

    it('picks the home page from the stored flag', () => {
        expect(run('/')).toEqual(['assets/AuthenticatedApp.js', 'assets/HomeV3.js', 'assets/shared.js']);
        document.head.querySelectorAll('link[rel="modulepreload"]').forEach((node) => node.remove());
        localStorage.setItem('redesign', 'false');
        expect(run('/')).toEqual(['assets/AuthenticatedApp.js', 'assets/HomeV2.js', 'assets/shared.js']);
    });

    it('matches the path below the base href', () => {
        const base = document.createElement('base');
        base.setAttribute('href', '/datahub/');
        document.head.appendChild(base);
        expect(run('/datahub/search')).toEqual([
            'assets/AuthenticatedApp.js',
            'assets/SearchRoutes.js',
            'assets/SearchPage.js',
        ]);
    });

    it('adds nothing without the actor cookie', () => {
        expect(run('/search', 'other=1')).toEqual([]);
    });
});
