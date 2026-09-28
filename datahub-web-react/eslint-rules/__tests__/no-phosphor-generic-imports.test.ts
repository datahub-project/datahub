import tsParser from '@typescript-eslint/parser';
import { Linter } from 'eslint';
import { describe, expect, it } from 'vitest';

// The rule is a CommonJS module (loaded by ESLint via eslint-plugin-rulesdir).
import rule from '../no-phosphor-generic-imports.js';

function lint(code: string): string[] {
    const linter = new Linter();
    linter.defineParser('ts', tsParser as never);
    linter.defineRule('t/no-phosphor-generic-imports', rule as never);
    return linter
        .verify(
            code,
            {
                parser: 'ts',
                parserOptions: { ecmaVersion: 2020, sourceType: 'module', ecmaFeatures: { jsx: true } },
                rules: { 't/no-phosphor-generic-imports': 'error' },
            },
            undefined,
        )
        .map((m) => m.message);
}

describe('no-phosphor-generic-imports', () => {
    describe('flagged cases (blocked)', () => {
        it.each([
            // Root imports
            "import { Icon } from '@phosphor-icons/react';",
            "import { Icon } from '@phosphor-icons/react/';",
            // Generic dist imports
            "import { Icon } from '@phosphor-icons/react/dist';",
            "import { Icon } from '@phosphor-icons/react/dist/';",
            // Generic folder imports (without specific icon)
            "import { Icon } from '@phosphor-icons/react/dist/csr';",
            "import { Icon } from '@phosphor-icons/react/dist/csr/';",
            "import { Icon } from '@phosphor-icons/react/dist/ssr';",
            "import { Icon } from '@phosphor-icons/react/dist/ssr/';",
            // lib folder (except lib/types which contains type definitions)
            "import { Icon } from '@phosphor-icons/react/dist/lib';",
            // index files
            "import { Icon } from '@phosphor-icons/react/dist/ssr/index';",
            // Export patterns
            "export { Icon } from '@phosphor-icons/react';",
            "export * from '@phosphor-icons/react/dist';",
            // Dynamic imports with strings
            "const icon = await import('@phosphor-icons/react/dist/csr');",
            // Dynamic imports with backticks (template literals)
            "const icon = await import(`@phosphor-icons/react/dist`);",
            // CommonJS with strings
            "const { Icon } = require('@phosphor-icons/react/dist');",
            // CommonJS with backticks
            "const { Icon } = require(`@phosphor-icons/react`);",
            // Overly broad paths that shouldn't match /dist/lib/types pattern
            "import { Icon } from '@phosphor-icons/react/dist/lib/types-old';",
        ])('flags %s', (code) => {
            const msgs = lint(code);
            expect(msgs).toHaveLength(1);
            expect(msgs[0]).toMatch(/Import Phosphor icons from individual icon paths/);
        });
    });

    describe('allowed cases', () => {
        it.each([
            // Specific CSR icon imports
            "import { CheckCircle } from '@phosphor-icons/react/dist/csr/CheckCircle';",
            "import { Icon } from '@phosphor-icons/react/dist/csr/Warning';",
            "import { Copy, Trash } from '@phosphor-icons/react/dist/csr/Copy';",
            // Specific SSR icon imports
            "import { CheckCircle } from '@phosphor-icons/react/dist/ssr/CheckCircle';",
            "import { Icon } from '@phosphor-icons/react/dist/ssr/Warning';",
            // Type imports from root
            "import type { Icon } from '@phosphor-icons/react';",
            "import type { IconWeight } from '@phosphor-icons/react';",
            // Type-only exports
            "export type { Icon } from '@phosphor-icons/react';",
            "export type * from '@phosphor-icons/react';",
            // Type definitions from /dist/lib/types
            "import { Icon as PhosphorIcon } from '@phosphor-icons/react/dist/lib/types';",
            "import type { Icon } from '@phosphor-icons/react/dist/lib/types';",
            // Specific icon imports with template literals in dynamic imports (allowed)
            "const CheckCircle = import(`@phosphor-icons/react/dist/csr/CheckCircle`);",
            // Non-phosphor imports
            "import { Button } from '@components';",
            "import { useState } from 'react';",
        ])('does not flag %s', (code) => {
            expect(lint(code)).toHaveLength(0);
        });
    });

    describe('edge cases', () => {
        it('allows multiple imports from the same specific icon', () => {
            const code = "import { CheckCircle as Circle } from '@phosphor-icons/react/dist/csr/CheckCircle';";
            expect(lint(code)).toHaveLength(0);
        });

        it('allows lowercase icon names', () => {
            const code = "import { warning } from '@phosphor-icons/react/dist/csr/warning';";
            expect(lint(code)).toHaveLength(0);
        });

        it('flags paths with special characters', () => {
            const code = "import { Icon } from '@phosphor-icons/react/dist/csr/Icon-Name';";
            expect(lint(code)).toHaveLength(1);
        });
    });
});
