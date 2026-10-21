// Pure helpers for mpack (see README.md): no I/O, so they are unit-tested directly.
import { createHash } from 'node:crypto';

export const sha256 = (data) => createHash('sha256').update(data).digest('hex');

// Vite's dev transform inlines a base64 source map in every module; it is most of the bytes.
const INLINE_SOURCE_MAP = /\n\/\/# sourceMappingURL=data:[^\n]*/g;

export function stripInlineSourceMap(code) {
    return code.replace(INLINE_SOURCE_MAP, '');
}

export function isScriptContentType(contentType) {
    return /javascript|typescript/.test(contentType || '');
}

// URLs that are modules mpack can pack: not Vite internals, not HMR-stamped copies, not mpack's own.
export function isPackableUrl(url) {
    return !url.startsWith('/@') && !url.startsWith('/__mpack/') && !url.includes('?t=');
}

export const barePath = (url) => url.split('?')[0];

// A chunk ends after a module whose path hash hits 1 in `every`, or once it reaches `maxBytes`. The
// boundary depends only on the module's path, so editing one module changes its own chunk only.
export function endsChunk(url, every = 120) {
    return parseInt(sha256(url).slice(0, 8), 16) % every === 0;
}

// modules: [{ url, hash, ctype, gz }] in load order. Returns chunks named by the hash of their module
// hashes (a Merkle node): [{ hash, mods: [[url, hash, gzLength, ctype]], parts: [gz...] }].
export function chunkModules(modules, { every = 120, maxBytes = 1 << 20, cut = endsChunk } = {}) {
    const chunks = [];
    let cur = null;
    for (const m of modules) {
        if (!cur) {
            cur = { mods: [], parts: [], size: 0 };
            chunks.push(cur);
        }
        cur.mods.push([m.url, m.hash, m.gz.length, m.ctype]);
        cur.parts.push(m.gz);
        cur.size += m.gz.length;
        if (cut(m.url, every) || cur.size >= maxBytes) cur = null;
    }
    return chunks.map(({ mods, parts }) => ({ hash: sha256(mods.map((x) => x[1]).join('')), mods, parts }));
}

// Root of a binary Merkle tree over the leaf hashes (an odd node pairs with itself); '' when empty.
export function merkleRoot(leaves) {
    let level = leaves;
    while (level.length > 1) {
        const next = [];
        for (let i = 0; i < level.length; i += 2) next.push(sha256(level[i] + (level[i + 1] ?? level[i])));
        level = next;
    }
    return level[0] ?? '';
}

// Modules to re-read after `paths` changed: the changed modules themselves and every module whose code
// mentions one of them (Vite rewrites an importer's import line with a version stamp).
export function affectedModules(order, bodies, paths) {
    const hit = new Set();
    for (const url of order) {
        const body = bodies.get(url);
        if (paths.has(barePath(url)) || (body && [...paths].some((p) => body.includes(p)))) hit.add(url);
    }
    return hit;
}

// All changed paths in one Vite HMR message, or null if the message isn't about changed modules.
export function changedPaths(message) {
    if (message.type === 'update') {
        return new Set((message.updates || []).flatMap((u) => [u.path, u.acceptedPath]).filter(Boolean));
    }
    if (message.type === 'full-reload' && message.path && message.path !== '*') return new Set([message.path]);
    return null;
}

export function viteWsToken(clientSource) {
    const m = clientSource.match(/wsToken = "([^"]+)"/);
    return m ? m[1] : '';
}

// Registers the service worker on the first visit; afterwards reloads the page once whenever the pack
// root changes (a source edit), so the worker fetches just the changed chunks.
export const BOOT_SCRIPT =
    "<script>(function(){if(!('serviceWorker' in navigator))return;" +
    "if(!navigator.serviceWorker.controller){navigator.serviceWorker.register('/__mpack/sw.js',{scope:'/'})" +
    '.then(function(){return navigator.serviceWorker.ready}).then(function(){location.reload()});return;}' +
    "var first=null,es=new EventSource('/__mpack/events');es.onmessage=function(e){" +
    'if(first===null){first=e.data;return;}if(e.data!==first){es.close();location.reload();}};})();</script>';

export function injectBoot(html) {
    return html.includes('<head>') ? html.replace('<head>', `<head>${BOOT_SCRIPT}`) : BOOT_SCRIPT + html;
}
