import { render } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { SyntaxHighlighter } from '@components/components/CodeBlock/highlighter';

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
});
