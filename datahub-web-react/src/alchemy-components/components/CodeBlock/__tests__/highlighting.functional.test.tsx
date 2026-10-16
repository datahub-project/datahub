import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { I18nextProvider } from 'react-i18next';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { CodeBlock } from '@components/components/CodeBlock/CodeBlock';

import themeV2 from '@conf/theme/themeV2';
import i18n from '@src/i18n/i18n';

vi.mock('@components/components/Toast', () => ({
    toast: {
        success: vi.fn(),
        error: vi.fn(),
    },
}));

function renderCodeBlock(ui: React.ReactElement) {
    return render(
        <ThemeProvider theme={themeV2}>
            <I18nextProvider i18n={i18n}>{ui}</I18nextProvider>
        </ThemeProvider>,
    );
}

function content(): HTMLElement {
    return screen.getByTestId('code-block-content');
}

/** jsdom reports inline colors as `rgb(...)`, while the theme stores hex. */
function hexToRgb(hex: string): string {
    const normalized = hex.replace('#', '');
    const r = Number.parseInt(normalized.slice(0, 2), 16);
    const g = Number.parseInt(normalized.slice(2, 4), 16);
    const b = Number.parseInt(normalized.slice(4, 6), 16);
    return `rgb(${r}, ${g}, ${b})`;
}

const keywordColor = hexToRgb(themeV2.colors.textBrand);

/** react-syntax-highlighter turns theme keys into inline colors and drops those class names. */
function keywordSpan(root: HTMLElement, keyword: string): HTMLElement | undefined {
    return [...root.querySelectorAll('span')].find((el) => el.textContent === keyword);
}

describe('CodeBlock highlighting', () => {
    it.each([
        ['sql', 'SELECT id FROM orders', 'SELECT', 'id'],
        ['snowflake', 'SELECT id FROM orders', 'SELECT', 'id'],
        ['python', 'def load():\n    total = 1', 'def', 'total'],
        ['javascript', 'const value = 1;', 'const', 'value'],
        ['graphql', 'query { user { name } }', 'query', 'user'],
        ['protobuf', 'message User { string name = 1; }', 'message', 'User'],
    ])('colors %s keywords and leaves identifiers plain', (language, code, keyword, identifier) => {
        renderCodeBlock(<CodeBlock code={code} language={language} showHeader={false} />);

        const root = content();
        expect(root.textContent).toContain(keyword);
        expect(keywordSpan(root, keyword)?.style.color).toBe(keywordColor);
        const plain = [...root.querySelectorAll('span')].find((el) => el.textContent?.trim() === identifier);
        expect(plain?.style.color ?? '').toBe('');
    });

    it.each([
        ['yaml', 'name: orders\ncount: 1'],
        ['yml', 'name: orders\ncount: 1'],
        ['json', '{\n  "name": "orders"\n}'],
        ['gql', 'query { user { name } }'],
        ['py', 'def load():\n    return 1'],
    ])('highlights %s instead of printing plain text', (language, code) => {
        renderCodeBlock(<CodeBlock code={code} language={language} showHeader={false} />);

        expect(content().textContent).toContain(code.split('\n')[0]?.split(' ')[0] ?? code);
        expect(content().querySelector('.token')).not.toBeNull();
    });

    it('still shows an unregistered language as text', () => {
        renderCodeBlock(<CodeBlock code="module Main where" language="haskell" showHeader={false} />);

        expect(content().textContent).toContain('module Main where');
    });

    it('formats SQL when Format is clicked', async () => {
        const user = userEvent.setup();
        const onChange = vi.fn();

        renderCodeBlock(<CodeBlock code="select id from orders" language="sql" onChange={onChange} />);

        await user.click(screen.getByTestId('code-block-format'));

        await waitFor(() => expect(onChange).toHaveBeenCalled());
        expect(onChange.mock.calls[0]?.[0]).toBe('SELECT\n  id\nFROM\n  orders\n');
    });
});
