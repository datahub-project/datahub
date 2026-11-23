import type { Plugin } from 'vite';
import { describe, expect, it } from 'vitest';

// eslint-disable-next-line import-alias/import-alias -- plugin tests sit next to the plugin, outside src aliases
import { styledComponentsPurePlugin } from './styledComponentsPurePlugin';

type TransformResult = { code: string } | null;

async function run(code: string, id: string): Promise<TransformResult> {
    const plugin = styledComponentsPurePlugin() as Plugin & {
        transform: (code: string, id: string) => Promise<TransformResult>;
    };
    return plugin.transform(code, id);
}

const TSX_SOURCE = `
import styled from 'styled-components';

const Box = styled.div<{ $on: boolean }>\`
    color: red;
\`;

export const Wrapped = styled(Box)\`
    margin: 0;
\`;

export default function Component(): JSX.Element {
    return <Box $on />;
}
`;

describe('styledComponentsPurePlugin', () => {
    it('only applies to production builds', () => {
        expect(styledComponentsPurePlugin().apply).toBe('build');
    });

    it('annotates styled declarations as pure in TSX files', async () => {
        const result = await run(TSX_SOURCE, '/src/app/Box.tsx');

        expect(result?.code.match(/\/\*#__PURE__\*\//g)).toHaveLength(2);
    });

    it('parses TypeScript files that use generics outside JSX', async () => {
        const source = `
import styled from 'styled-components';

export const Row = styled.div\`
    display: flex;
\`;

export const identity = <T,>(value: T): T => value;
`;
        const result = await run(source, '/src/app/styled.ts');

        expect(result?.code).toContain('/*#__PURE__*/');
    });

    it('does not add displayName, fileName or componentId extras', async () => {
        const result = await run(TSX_SOURCE, '/src/app/Box.tsx');

        expect(result?.code).not.toContain('withConfig');
        expect(result?.code).not.toContain('displayName');
        expect(result?.code).not.toContain('componentId');
    });

    it('skips files that do not import styled-components', async () => {
        expect(await run('export const a = 1;', '/src/app/a.ts')).toBeNull();
    });

    it('skips dependencies and non-source files', async () => {
        expect(await run(TSX_SOURCE, '/repo/node_modules/pkg/index.js')).toBeNull();
        expect(await run(TSX_SOURCE, '/src/app/Box.css')).toBeNull();
    });

    it('ignores query suffixes when matching the file extension', async () => {
        const result = await run(TSX_SOURCE, '/src/app/Box.tsx?t=123');

        expect(result?.code).toContain('/*#__PURE__*/');
    });
});
