import { render } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { SyntaxHighlighter, loadHighlightLanguage } from '@components/components/CodeBlock/highlighter';

describe('syntax highlighter', () => {
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

    it('registers an uncommon grammar on demand', async () => {
        expect(await loadHighlightLanguage('java')).toBe(true);

        const { container } = render(<SyntaxHighlighter language="java">public class Main {}</SyntaxHighlighter>);
        expect(container.querySelector('.token.keyword')?.textContent).toBe('public');
    });

    it('reports languages without a grammar', async () => {
        expect(await loadHighlightLanguage('not-a-language')).toBe(false);
        expect(await loadHighlightLanguage('constructor')).toBe(false);
    });
});
