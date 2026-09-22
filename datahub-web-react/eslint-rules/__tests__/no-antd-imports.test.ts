import path from 'path';
import tsParser from '@typescript-eslint/parser';
import { Linter } from 'eslint';
import { describe, expect, it } from 'vitest';

// The rule is a CommonJS module (loaded by ESLint via eslint-plugin-rulesdir).
import rule from '../no-antd-imports.js';

function lint(code: string, ruleOptions: Record<string, unknown> = { allowedFiles: [] }, filename?: string): string[] {
    const linter = new Linter();
    linter.defineParser('ts', tsParser as never);
    linter.defineRule('t/no-antd-imports', rule as never);
    return linter
        .verify(
            code,
            {
                parser: 'ts',
                parserOptions: { ecmaVersion: 2020, sourceType: 'module', ecmaFeatures: { jsx: true } },
                rules: { 't/no-antd-imports': ['error', ruleOptions] },
            },
            filename ? { filename } : undefined,
        )
        .map((m) => m.message);
}

describe('no-antd-imports', () => {
    it.each([
        "import { Button } from 'antd';",
        "import { Form } from 'antd/es/form';",
        "import Button from 'antd/es/button';",
        "import type { DropdownProps } from 'antd';",
        "export { Button } from 'antd';",
        "export * from 'antd';",
        "const antd = require('antd');",
        "const Form = require('antd/es/form');",
        "const load = () => import('antd');",
    ])('flags %s', (code) => {
        const msgs = lint(code);
        expect(msgs).toHaveLength(1);
        expect(msgs[0]).toMatch(/alchemy components from `@components`/);
    });

    it.each([
        "import { Button } from '@components';",
        "import { Something } from 'antd-style';",
        "import { UserOutlined } from '@ant-design/icons';",
        "const note = 'import { Button } from \\'antd\\'';",
    ])('does not flag %s', (code) => {
        expect(lint(code)).toHaveLength(0);
    });

    it('does not flag antd in a grandfathered file', () => {
        const filename = path.resolve('/tmp/grandfathered-antd.tsx');
        expect(lint("import { Button } from 'antd';", { allowedFiles: [filename] }, filename)).toHaveLength(0);
    });

    it('still flags antd when a different file is grandfathered', () => {
        const allowed = path.resolve('/tmp/grandfathered-antd.tsx');
        const other = path.resolve('/tmp/new-file.tsx');
        const msgs = lint("import { Button } from 'antd';", { allowedFiles: [allowed] }, other);
        expect(msgs).toHaveLength(1);
    });

    it('skips enforcement when the git baseline ref is missing', () => {
        const filename = path.resolve('/tmp/new-antd-file.tsx');
        expect(
            lint("import { Button } from 'antd';", { baselineRef: 'refs/does-not-exist-antd-test' }, filename),
        ).toHaveLength(0);
    });
});
