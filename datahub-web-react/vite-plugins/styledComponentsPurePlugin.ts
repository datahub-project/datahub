import { transformAsync } from '@babel/core';
import type { Plugin } from 'vite';

const SOURCE_EXTENSION = /\.[cm]?[jt]sx?$/;
const STYLED_COMPONENTS_IMPORT = /from\s+['"]styled-components['"]/;

// Only `pure` is wanted. displayName, fileName and ssr default to on and would add
// withConfig() calls and component ids to every styled component. The SWC-only build
// never emitted them, so they stay off to keep runtime behavior unchanged.
const STYLED_COMPONENTS_PLUGIN_OPTIONS = { pure: true, displayName: false, fileName: false, ssr: false };

function parserPlugins(filePath: string): ('typescript' | 'jsx')[] {
    if (filePath.endsWith('.ts')) return ['typescript'];
    if (filePath.endsWith('.tsx')) return ['typescript', 'jsx'];
    return ['jsx'];
}

/**
 * SWC does not run babel-plugin-styled-components, so styled calls never get the PURE
 * annotation and rollup/esbuild keep unused styled components in the bundle.
 * Run only that plugin before SWC, on files that import styled-components.
 * Build only: dev and Vitest keep the SWC output, and tests that mock styled-components
 * are unaffected.
 */
export function styledComponentsPurePlugin(): Plugin {
    return {
        name: 'styled-components-pure',
        apply: 'build',
        enforce: 'pre',
        async transform(code, id) {
            const filePath = id.split('?')[0];
            if (filePath.includes('/node_modules/') || !SOURCE_EXTENSION.test(filePath)) return null;
            if (!STYLED_COMPONENTS_IMPORT.test(code)) return null;

            const result = await transformAsync(code, {
                filename: filePath,
                babelrc: false,
                configFile: false,
                sourceMaps: true,
                parserOpts: { plugins: parserPlugins(filePath) },
                plugins: [['babel-plugin-styled-components', STYLED_COMPONENTS_PLUGIN_OPTIONS]],
            });
            if (!result?.code) return null;
            return { code: result.code, map: result.map };
        },
    };
}
