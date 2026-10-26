import type { Plugin } from 'vite';

/**
 * When the `actor` cookie is already set, start downloading the authenticated
 * shell (AuthenticatedApp and the chunks it statically imports, chiefly the
 * shared provider graph) in parallel with the entry module.
 *
 * Those chunks are dynamic imports, so Vite will not modulepreload them. Leaving
 * the discovery to `import()` serializes their download behind entry evaluation.
 * A static `<link rel="modulepreload">` would also fetch them on /login, which
 * is the page that benefits from them staying off the first load. The inline
 * script checks the same cookie `checkAuthStatus` uses and inserts the links
 * only then, before the entry module script is parsed.
 *
 * The same holds one level down. The shell lazy-loads the current page, and that
 * request only starts once the shell has finished evaluating. For the routes in
 * ROUTE_PRELOADS the script also preloads the page chunks the shell will import,
 * so they download while the shell evaluates.
 */

const AUTH_SHELL_MODULE = /(?:^|\/)AuthenticatedApp\.tsx$/;

export interface RoutePreloadTarget {
    /** Matched against location.pathname with the <base> path removed. */
    path: RegExp;
    /** Lazy modules the route imports before it renders, outermost first. */
    modules: RegExp[];
    /** Only when localStorage holds this value, for routes that pick a page from a stored flag. */
    storage?: { key: string; value: string };
}

const SEARCH_ROUTES_MODULE = /(?:^|\/)app\/SearchRoutes\.tsx$/;

// Page modules per route, as ProtectedRoutes and SearchRoutes lazy-load them.
// The first matching entry wins.
export const ROUTE_PRELOADS: RoutePreloadTarget[] = [
    // useShowHomePageRedesign reads this flag until app config loads. A missing value means V3.
    {
        path: /^\/$/,
        modules: [/(?:^|\/)app\/homeV2\/HomePage\.tsx$/],
        storage: { key: 'showHomePageRedesign', value: 'false' },
    },
    { path: /^\/$/, modules: [/(?:^|\/)app\/homeV3\/HomePage\.tsx$/] },
    { path: /^\/search(?:\/|$)/, modules: [SEARCH_ROUTES_MODULE, /(?:^|\/)app\/searchV2\/SearchPage\.tsx$/] },
    // Entity types whose profile is the shared EntityProfile behind EntityPage.
    {
        path: /^\/(?:dataset|chart|dashboard|container|pipelines|tasks)\//,
        modules: [
            SEARCH_ROUTES_MODULE,
            /(?:^|\/)app\/entityV2\/EntityPage\.tsx$/,
            /(?:^|\/)app\/entityV2\/shared\/containers\/profile\/EntityProfile\.tsx$/,
        ],
    },
];

export interface PreloadChunk {
    type: string;
    fileName: string;
    isEntry?: boolean;
    imports?: string[];
    facadeModuleId?: string | null;
    modules?: Record<string, unknown>;
}

export interface RoutePreload {
    path: RegExp;
    files: string[];
    storage?: { key: string; value: string };
}

function findModuleChunk(chunks: PreloadChunk[], pattern: RegExp): PreloadChunk | undefined {
    return chunks.find(
        (chunk) =>
            (!!chunk.facadeModuleId && pattern.test(chunk.facadeModuleId)) ||
            Object.keys(chunk.modules || {}).some((id) => pattern.test(id)),
    );
}

/** `roots` first, then the chunks they statically import. Skips entries and `exclude`. */
function collectStaticGraph(chunks: PreloadChunk[], roots: PreloadChunk[], exclude: string[] = []): string[] {
    const byFile = new Map(chunks.map((chunk) => [chunk.fileName, chunk]));
    const files: string[] = [];
    const seen = new Set<string>(exclude);
    const include = (fileName: string): PreloadChunk | undefined => {
        if (seen.has(fileName)) return undefined;
        seen.add(fileName);
        const chunk = byFile.get(fileName);
        // Entry chunks are already <script> tags. Don't walk into them: their
        // static imports are the eager graph, including chunks login needs.
        if (!chunk || chunk.isEntry) return undefined;
        files.push(fileName);
        return chunk;
    };
    const visit = (fileName: string) => {
        const chunk = include(fileName);
        (chunk?.imports || []).forEach((imported) => visit(imported));
    };
    // Roots go first so each page chunk is requested before its dependencies.
    const included = roots.map((root) => include(root.fileName));
    included.forEach((chunk) => (chunk?.imports || []).forEach((imported) => visit(imported)));
    return files;
}

function bundleChunks(bundle: Record<string, PreloadChunk>): PreloadChunk[] {
    return Object.values(bundle).filter((item) => item.type === 'chunk');
}

export function collectAuthShellPreloadFiles(bundle: Record<string, PreloadChunk>): string[] {
    const chunks = bundleChunks(bundle);
    const shell = findModuleChunk(chunks, AUTH_SHELL_MODULE);
    if (!shell) return [];
    return collectStaticGraph(chunks, [shell]);
}

/**
 * Per route, its page chunks and their static imports, minus `shellFiles`. A route
 * with a module missing from the bundle is skipped and reported to `onMissing`.
 */
export function collectRoutePreloadFiles(
    bundle: Record<string, PreloadChunk>,
    shellFiles: string[],
    targets: RoutePreloadTarget[] = ROUTE_PRELOADS,
    onMissing: (pattern: RegExp) => void = () => {},
): RoutePreload[] {
    const chunks = bundleChunks(bundle);
    const routes: RoutePreload[] = [];
    targets.forEach((target) => {
        const missing = target.modules.find((pattern) => !findModuleChunk(chunks, pattern));
        if (missing) {
            onMissing(missing);
            return;
        }
        const roots = target.modules
            .map((pattern) => findModuleChunk(chunks, pattern))
            .filter((chunk): chunk is PreloadChunk => !!chunk);
        const files = collectStaticGraph(chunks, roots, shellFiles);
        if (files.length === 0) return;
        routes.push(
            target.storage ? { path: target.path, files, storage: target.storage } : { path: target.path, files },
        );
    });
    return routes;
}

/**
 * Files are listed once and routes refer to them by index, since the route graphs
 * overlap (search and entity pages share SearchRoutes). The shell files are the
 * first `files.length` entries. Routes match the path below the <base> href, the
 * way the router sees it.
 */
export function renderAuthShellPreloadScript(files: string[], routes: RoutePreload[] = []): string {
    if (files.length === 0) return '';
    const table = Array.from(new Set([...files, ...routes.flatMap((route) => route.files)]));
    const index = new Map(table.map((file, i) => [file, i]));
    const routeList = routes
        .map((route) => {
            const ids = JSON.stringify(route.files.map((file) => index.get(file)));
            const storage = route.storage
                ? `,${JSON.stringify(route.storage.key)},${JSON.stringify(route.storage.value)}`
                : '';
            return `[${String(route.path)},${ids}${storage}]`;
        })
        .join(',');
    const routeMatch = routes.length
        ? `var routes=[${routeList}];` +
          'var base=document.querySelector("base"),root=base&&base.getAttribute("href")||"/",' +
          'path=location.pathname;if(root.charAt(0)!=="/")root="/";' +
          'if(root.charAt(root.length-1)!=="/")root+="/";' +
          'if(path.indexOf(root)===0)path=path.slice(root.length-1);' +
          'for(var k=0;k<routes.length;k++){var route=routes[k];' +
          'if(route[0].test(path)&&(!route[2]||localStorage.getItem(route[2])===route[3])){' +
          'for(j=0;j<route[1].length;j++)preload(files[route[1][j]]);break;}}'
        : '';
    return (
        '<script>(function(){try{var parts=document.cookie.split(";");' +
        'for(var i=0;i<parts.length;i++){var part=parts[i].trim();' +
        'if(part.indexOf("actor=")===0&&part.length>6){' +
        `var files=${JSON.stringify(table)};` +
        'var preload=function(href){var link=document.createElement("link");' +
        'link.rel="modulepreload";link.crossOrigin="";link.href=href;' +
        'document.head.appendChild(link);};' +
        `for(var j=0;j<${files.length};j++)preload(files[j]);` +
        `${routeMatch}return;}}}catch(e){}})();</script>`
    );
}

export function authShellPreloadPlugin(): Plugin {
    return {
        name: 'auth-shell-preload',
        apply: 'build',
        transformIndexHtml: {
            order: 'post',
            handler(html, ctx) {
                const filename = ctx.filename || ctx.path || '';
                if (!filename.endsWith('index.html')) return html;
                const bundle = ctx.bundle as Record<string, PreloadChunk> | undefined;
                if (!bundle) return html;
                const shellFiles = collectAuthShellPreloadFiles(bundle);
                const routes = collectRoutePreloadFiles(bundle, shellFiles, ROUTE_PRELOADS, (pattern) => {
                    // eslint-disable-next-line no-console
                    console.warn(`[auth-shell-preload] no chunk for ${pattern}; route preload skipped`);
                });
                const script = renderAuthShellPreloadScript(shellFiles, routes);
                if (!script) {
                    // eslint-disable-next-line no-console
                    console.warn('[auth-shell-preload] AuthenticatedApp chunk not found; no preload injected');
                    return html;
                }
                const moduleScript = '<script type="module"';
                if (html.includes(moduleScript)) return html.replace(moduleScript, `${script}${moduleScript}`);
                return html.replace('</head>', `${script}</head>`);
            },
        },
    };
}
