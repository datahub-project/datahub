import { ComponentType, LazyExoticComponent, lazy } from 'react';

/**
 * Route-level code splitting helpers.
 *
 * A `import()` is a chunk boundary. If the imported module can reach back to
 * the importer, modulepreload can evaluate the async chunk before the importer
 * finishes initializing, which is a TDZ crash — and it depends on which route
 * loaded the chunk and which chunk won the race. The static check in
 * chunkBoundaryCycles.test.ts is what rules that out. Route tests only sample
 * the routes they open.
 *
 * Do not add static `manualChunks` splits of `src/` to force extra app chunks.
 * That cuts static cycles the same way and is the failure mode documented in
 * vite.config.ts.
 */

export function lazyDefault<P extends object>(
    importer: () => Promise<{ default: ComponentType<P> }>,
): LazyExoticComponent<ComponentType<P>> {
    return lazy(importer);
}

export function lazyNamed<P extends object, K extends string>(
    importer: () => Promise<Record<K, ComponentType<P>>>,
    name: K,
): LazyExoticComponent<ComponentType<P>> {
    const load = (): Promise<{ default: ComponentType<P> }> => importer().then((module) => ({ default: module[name] }));
    return lazy(load);
}
