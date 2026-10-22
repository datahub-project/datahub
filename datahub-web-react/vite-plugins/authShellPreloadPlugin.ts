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
 * Home, search, and dataset each add only that route's own chunk, not the
 * chunks it statically imports. Preloading the whole static graph starts those
 * files at high priority before the stylesheet is parsed, and that delayed the
 * CSS and the font the largest paint was waiting on. The browser still
 * discovers the route chunk's imports once that one file arrives. Other routes
 * still get the shell only, so a settings visit does not download the dataset
 * profile.
 *
 * The stylesheet link is moved ahead of this script. It is render-blocking, and
 * the body font is declared there, so it has to be discovered before the
 * modulepreloads.
 */

const AUTH_SHELL_MODULE = /(?:^|\/)AuthenticatedApp\.tsx$/;
const HOME_MODULE = /(?:^|\/)homeV3\/HomePage\.tsx$/;
const SEARCH_ROUTES_MODULE = /(?:^|\/)SearchRoutes\.tsx$/;

export type RoutePreloadKey = 'shell' | 'home' | 'search' | 'dataset';

export interface RoutePreloadGroups {
    shell: string[];
    home: string[];
    search: string[];
    dataset: string[];
}

export interface PreloadChunk {
    type: string;
    fileName: string;
    isEntry?: boolean;
    imports?: string[];
    facadeModuleId?: string | null;
    modules?: Record<string, unknown>;
}

function chunkMatches(chunk: PreloadChunk, pattern: RegExp): boolean {
    return (
        (!!chunk.facadeModuleId && pattern.test(chunk.facadeModuleId)) ||
        Object.keys(chunk.modules || {}).some((id) => pattern.test(id))
    );
}

function findChunk(chunks: PreloadChunk[], pattern: RegExp): PreloadChunk | undefined {
    return chunks.find((chunk) => chunkMatches(chunk, pattern));
}

// Static import closure, including each start chunk. Entry chunks are already
// <script> tags; don't walk into them, because their static imports are the
// eager graph login needs.
export function collectStaticPreloadFiles(byFile: Map<string, PreloadChunk>, startFileNames: string[]): string[] {
    const files: string[] = [];
    const seen = new Set<string>();
    const visit = (fileName: string) => {
        if (seen.has(fileName)) return;
        seen.add(fileName);
        const chunk = byFile.get(fileName);
        if (!chunk || chunk.isEntry) return;
        files.push(fileName);
        (chunk.imports || []).forEach((imported) => visit(imported));
    };
    startFileNames.forEach((fileName) => visit(fileName));
    return files;
}

// The route chunk itself, not its static imports. Walking the closure queues
// dozens of high-priority script requests before the parser reaches the CSS.
function entryFiles(chunks: PreloadChunk[], patterns: RegExp[]): string[] {
    const files: string[] = [];
    const seen = new Set<string>();
    patterns.forEach((pattern) => {
        const chunk = findChunk(chunks, pattern);
        if (!chunk || chunk.isEntry || seen.has(chunk.fileName)) return;
        seen.add(chunk.fileName);
        files.push(chunk.fileName);
    });
    return files;
}

function unionFiles(groups: string[][]): string[] {
    const files: string[] = [];
    const seen = new Set<string>();
    groups.forEach((group) => {
        group.forEach((fileName) => {
            if (seen.has(fileName)) return;
            seen.add(fileName);
            files.push(fileName);
        });
    });
    return files;
}

export function collectAuthShellPreloadFiles(bundle: Record<string, PreloadChunk>): string[] {
    return collectRoutePreloadGroups(bundle).shell;
}

export function collectRoutePreloadGroups(bundle: Record<string, PreloadChunk>): RoutePreloadGroups {
    const chunks = Object.values(bundle).filter((item) => item.type === 'chunk');
    const byFile = new Map(chunks.map((chunk) => [chunk.fileName, chunk]));
    const closure = (patterns: RegExp[]) =>
        collectStaticPreloadFiles(
            byFile,
            patterns
                .map((pattern) => findChunk(chunks, pattern)?.fileName)
                .filter((fileName): fileName is string => !!fileName),
        );
    const shell = closure([AUTH_SHELL_MODULE]);
    return {
        shell,
        home: unionFiles([shell, entryFiles(chunks, [HOME_MODULE])]),
        // SearchRoutes is the direct async boundary from ProtectedRoutes for
        // both paths. SearchPage and EntityPage are later hops; starting them
        // early did not improve paint and made dataset tail metrics noisier.
        search: unionFiles([shell, entryFiles(chunks, [SEARCH_ROUTES_MODULE])]),
        dataset: unionFiles([shell, entryFiles(chunks, [SEARCH_ROUTES_MODULE])]),
    };
}

export function routePreloadKey(pathname: string, baseHref: string | null | undefined): RoutePreloadKey {
    let base = baseHref || '/';
    if (!base.startsWith('/') || base.includes('@')) base = '/';
    const trimmedBase = base.length > 1 ? base.replace(/\/$/, '') : '';
    let path = pathname || '/';
    if (trimmedBase && path.startsWith(trimmedBase)) path = path.slice(trimmedBase.length) || '/';
    if (path.length > 1 && path.endsWith('/')) path = path.slice(0, -1);
    if (path === '/' || path === '') return 'home';
    if (path === '/search' || path.startsWith('/search/')) return 'search';
    if (path === '/dataset' || path.startsWith('/dataset/')) return 'dataset';
    return 'shell';
}

export function renderAuthShellPreloadScript(files: string[]): string {
    return renderRoutePreloadScript({ shell: files, home: files, search: files, dataset: files });
}

export function renderRoutePreloadScript(groups: RoutePreloadGroups): string {
    const empty =
        groups.shell.length === 0 &&
        groups.home.length === 0 &&
        groups.search.length === 0 &&
        groups.dataset.length === 0;
    if (empty) return '';
    const payload = JSON.stringify(groups);
    return (
        '<script>(function(){try{var parts=document.cookie.split(";");var actor=false;' +
        'for(var i=0;i<parts.length;i++){var part=parts[i].trim();' +
        'if(part.indexOf("actor=")===0&&part.length>6){actor=true;break;}}' +
        'if(!actor)return;' +
        'var baseEl=document.querySelector("base");var base=(baseEl&&baseEl.getAttribute("href"))||"/";' +
        'if(base.charAt(0)!=="/"||base.indexOf("@")!==-1)base="/";' +
        'var trimmed=base.length>1?base.replace(/\\/$/,""):"";' +
        'var path=location.pathname||"/";' +
        'if(trimmed&&path.indexOf(trimmed)===0)path=path.slice(trimmed.length)||"/";' +
        'if(path.length>1&&path.charAt(path.length-1)==="/")path=path.slice(0,-1);' +
        `var groups=${payload};var files=groups.shell;` +
        'if(path==="/"||path==="")files=groups.home;' +
        'else if(path==="/search"||path.indexOf("/search/")===0)files=groups.search;' +
        'else if(path==="/dataset"||path.indexOf("/dataset/")===0)files=groups.dataset;' +
        'for(var j=0;j<files.length;j++){var link=document.createElement("link");' +
        'link.rel="modulepreload";link.crossOrigin="";link.href=files[j];' +
        'document.head.appendChild(link);}}catch(e){}})();</script>'
    );
}

const STYLESHEET_LINK = /<link\b[^>]*\brel="stylesheet"[^>]*>/g;

// Render-blocking CSS has to be in the document before the preload script runs.
// The script inserts modulepreloads synchronously, and those links are high priority.
export function promoteStylesheetsBefore(html: string, marker: string): string {
    if (!marker || !html.includes(marker)) return html;
    const links: string[] = [];
    const stripped = html.replace(STYLESHEET_LINK, (tag) => {
        links.push(tag);
        return '';
    });
    if (links.length === 0) return html;
    return stripped.replace(marker, `${links.join('')}${marker}`);
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
                const groups = collectRoutePreloadGroups(bundle);
                if (groups.shell.length === 0) {
                    // eslint-disable-next-line no-console
                    console.warn('[auth-shell-preload] AuthenticatedApp chunk not found; no preload injected');
                    return html;
                }
                const script = renderRoutePreloadScript(groups);
                const moduleScript = '<script type="module"';
                const withScript = html.includes(moduleScript)
                    ? html.replace(moduleScript, `${script}${moduleScript}`)
                    : html.replace('</head>', `${script}</head>`);
                return promoteStylesheetsBefore(withScript, script);
            },
        },
    };
}
