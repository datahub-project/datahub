import { Linter } from 'eslint';
import { describe, expect, it } from 'vitest';

import tsParser from '@typescript-eslint/parser';

// The rule is a CommonJS module (loaded by ESLint via eslint-plugin-rulesdir).
import rule from '../use-app-wrappers.js';

const COMPONENTS = [
    {
        name: 'Editor',
        folder: 'Editor',
        wrappers: ['src/app/sharedV2/editor/Editor.tsx', 'src/app/chat/ChatEditor.tsx'],
    },
    { name: 'Tabs', folder: 'Tabs', wrappers: ['src/app/sharedV2/tabs/Tabs.tsx'] },
];

const APP_FILE = '/repo/datahub-web-react/src/app/entityV2/Page.tsx';

function lint(code: string, filename = APP_FILE): string[] {
    const linter = new Linter();
    linter.defineParser('ts', tsParser as never);
    linter.defineRule('t/use-app-wrappers', rule as never);
    return linter
        .verify(
            code,
            {
                parser: 'ts',
                parserOptions: { ecmaVersion: 2020, sourceType: 'module', ecmaFeatures: { jsx: true } },
                rules: { 't/use-app-wrappers': ['error', { components: COMPONENTS }] },
            },
            { filename },
        )
        .map((m) => m.message);
}

describe('use-app-wrappers', () => {
    it.each([
        "import { Editor } from '@components';",
        "import { Editor } from '@components/components/Editor';",
        "import { Editor } from '@components/components/Editor/EditorImpl';",
        "import { Editor as MarkdownEditor } from '@src/alchemy-components/components/Editor/Editor';",
        "export { Editor } from '@components';",
        "import * as EditorModule from '@components/components/Editor/Editor';",
        "export * from '@components/components/Editor';",
        "const load = () => import('@components/components/Editor/EditorImpl');",
    ])('flags %s in app code', (code) => {
        expect(lint(code)).toEqual([
            "Import Editor from '@app/sharedV2/editor/Editor': the app wrapper adds app behavior to the alchemy component.",
        ]);
    });

    it('flags only the wrapped name in a mixed import', () => {
        expect(lint("import { Button, Editor, Tabs } from '@components';")).toEqual([
            "Import Editor from '@app/sharedV2/editor/Editor': the app wrapper adds app behavior to the alchemy component.",
            "Import Tabs from '@app/sharedV2/tabs/Tabs': the app wrapper adds app behavior to the alchemy component.",
        ]);
    });

    it.each([
        "import { Button } from '@components';",
        "import { EditorProps } from '@components/components/Editor/types';",
        "import type { Editor } from '@components';",
        "import Editor from '@monaco-editor/react';",
        "import { Editor } from '@app/sharedV2/editor/Editor';",
        "import * as Alchemy from '@components';",
        "import type * as EditorTypes from '@components/components/Editor/types';",
    ])('allows %s', (code) => {
        expect(lint(code)).toEqual([]);
    });

    it('lets a wrapper import its own component but not another wrapped one', () => {
        const wrapper = '/repo/datahub-web-react/src/app/sharedV2/editor/Editor.tsx';

        expect(lint("import { Editor } from '@components/components/Editor';", wrapper)).toEqual([]);
        expect(lint("import { Tabs } from '@components';", wrapper)).toHaveLength(1);
    });

    it('lets every listed wrapper import the component and points app code to the first one', () => {
        const secondWrapper = '/repo/datahub-web-react/src/app/chat/ChatEditor.tsx';

        expect(lint("import { Editor } from '@components';", secondWrapper)).toEqual([]);
        expect(lint("import { Editor } from '@components';")[0]).toContain("'@app/sharedV2/editor/Editor'");
    });

    it('checks the folder, not the name', () => {
        expect(lint("import { Editor } from '@components/components/Tabs/Tabs';")).toEqual([]);
    });

    it('ignores alchemy files', () => {
        const alchemyFile = '/repo/datahub-web-react/src/alchemy-components/components/Editor/Editor.stories.tsx';

        expect(lint("import { Editor } from '@components/components/Editor/Editor';", alchemyFile)).toEqual([]);
    });
});
