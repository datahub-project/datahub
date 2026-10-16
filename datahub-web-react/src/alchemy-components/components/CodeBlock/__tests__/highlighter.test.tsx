import { render } from '@testing-library/react';
import fs from 'fs';
import path from 'path';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { SyntaxHighlighter } from '@components/components/CodeBlock/highlighter';

const CODE_BLOCK_DIR = path.resolve(__dirname, '..');

function read(relPath: string): string {
    return fs.readFileSync(path.join(CODE_BLOCK_DIR, relPath), 'utf8');
}

const SHELL_IMPORTERS = [
    'CodeBlock.tsx',
    'CodeBlockEditor.tsx',
    'highlighter.ts',
    'formatCode.ts',
    '../Editor/extensions/fileDragDrop/FileNodeView.tsx',
    '../Editor/toolbar/CodeBlockToolbar.tsx',
];

describe('syntax highlighter bundle', () => {
    it('does not import the full Prism build, refractor, or sql-formatter', () => {
        SHELL_IMPORTERS.forEach((relPath) => {
            const src = read(relPath);
            expect(src).not.toMatch(/from\s+['"]react-syntax-highlighter['"]/);
            expect(src).not.toMatch(/from\s+['"]refractor['"]/);
            expect(src).not.toMatch(/from\s+['"]sql-formatter['"]/);
        });

        const highlighter = read('highlighter.ts');
        expect(highlighter).toMatch(/prism-light/);
        expect(highlighter).not.toMatch(/dist\/esm\/prism['"]/);

        const formatCode = read('formatCode.ts');
        expect(formatCode).toMatch(/import\('sql-formatter'\)/);
    });

    it('highlights SQL with the registered grammar', () => {
        const { container } = render(<SyntaxHighlighter language="sql">SELECT 1</SyntaxHighlighter>);

        expect(container.querySelector('.token.keyword')?.textContent).toBe('SELECT');
        expect(container.textContent).toContain('1');
    });

    it('highlights a yaml alias without a separate grammar', () => {
        const { container } = render(<SyntaxHighlighter language="yml">key: 1</SyntaxHighlighter>);

        expect(container.textContent).toContain('key');
        expect(container.querySelector('.token')).not.toBeNull();
    });
});
