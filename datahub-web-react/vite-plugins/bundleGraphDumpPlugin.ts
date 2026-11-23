import * as fs from 'fs';
import * as path from 'path';
import type { Plugin } from 'vite';

/**
 * Shape consumed by `e2e-test/perf/vitals/bundle-inventory.mjs --graph`
 * (`classifyReachability`): the Rollup module graph plus per-chunk rendered bytes.
 */
export type BundleGraph = {
    modules: { id: string; isEntry: boolean; importedIds: string[]; dynamicallyImportedIds: string[] }[];
    chunks: { fileName: string; modules: Record<string, number> }[];
};

/**
 * Writes the module graph to `outFile` at the end of a build so the inventory can
 * tell which eager modules are only reachable through `import()`. A no-op when
 * `outFile` is empty, so ordinary builds are unaffected.
 */
export function bundleGraphDumpPlugin(outFile: string | undefined): Plugin {
    return {
        name: 'bundle-graph-dump',
        apply: 'build',
        generateBundle(_options, bundle) {
            if (!outFile) {
                return;
            }
            const modules = Array.from(this.getModuleIds()).flatMap((id) => {
                const info = this.getModuleInfo(id);
                return info
                    ? [
                          {
                              id,
                              isEntry: info.isEntry,
                              importedIds: [...info.importedIds],
                              dynamicallyImportedIds: [...info.dynamicallyImportedIds],
                          },
                      ]
                    : [];
            });
            const chunks = Object.values(bundle).flatMap((item) =>
                item.type === 'chunk'
                    ? [
                          {
                              fileName: item.fileName,
                              modules: Object.fromEntries(
                                  Object.entries(item.modules).map(([id, rendered]) => [id, rendered.renderedLength]),
                              ),
                          },
                      ]
                    : [],
            );
            const graph: BundleGraph = { modules, chunks };
            const resolved = path.resolve(outFile);
            fs.mkdirSync(path.dirname(resolved), { recursive: true });
            fs.writeFileSync(resolved, JSON.stringify(graph));
        },
    };
}
