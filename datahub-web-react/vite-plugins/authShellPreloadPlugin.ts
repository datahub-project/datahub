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
 */

const AUTH_SHELL_MODULE = /(?:^|\/)AuthenticatedApp\.tsx$/;

export interface PreloadChunk {
    type: string;
    fileName: string;
    isEntry?: boolean;
    imports?: string[];
    facadeModuleId?: string | null;
    modules?: Record<string, unknown>;
}

export function collectAuthShellPreloadFiles(bundle: Record<string, PreloadChunk>): string[] {
    const chunks = Object.values(bundle).filter((item) => item.type === 'chunk');
    const byFile = new Map(chunks.map((chunk) => [chunk.fileName, chunk]));
    const shell = chunks.find(
        (chunk) =>
            (!!chunk.facadeModuleId && AUTH_SHELL_MODULE.test(chunk.facadeModuleId)) ||
            Object.keys(chunk.modules || {}).some((id) => AUTH_SHELL_MODULE.test(id)),
    );
    if (!shell) return [];

    const files: string[] = [];
    const seen = new Set<string>();
    const visit = (fileName: string) => {
        if (seen.has(fileName)) return;
        seen.add(fileName);
        const chunk = byFile.get(fileName);
        // Entry chunks are already <script> tags. Don't walk into them: their
        // static imports are the eager graph, including chunks login needs.
        if (!chunk || chunk.isEntry) return;
        files.push(fileName);
        (chunk.imports || []).forEach((imported) => visit(imported));
    };
    visit(shell.fileName);
    return files;
}

export function renderAuthShellPreloadScript(files: string[]): string {
    if (files.length === 0) return '';
    const list = JSON.stringify(files);
    return (
        '<script>(function(){try{var parts=document.cookie.split(";");' +
        'for(var i=0;i<parts.length;i++){var part=parts[i].trim();' +
        'if(part.indexOf("actor=")===0&&part.length>6){' +
        `var files=${list};` +
        'for(var j=0;j<files.length;j++){var link=document.createElement("link");' +
        'link.rel="modulepreload";link.crossOrigin="";link.href=files[j];' +
        'document.head.appendChild(link);}return;}}}catch(e){}})();</script>'
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
                const script = renderAuthShellPreloadScript(collectAuthShellPreloadFiles(bundle));
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
