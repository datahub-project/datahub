import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { I18nextProvider } from 'react-i18next';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { CodeBlock } from '@components/components/CodeBlock/CodeBlock';
import { toast } from '@components/components/Toast';

import themeV2 from '@conf/theme/themeV2';
import i18n from '@src/i18n/i18n';

vi.mock('sql-formatter', () => {
    throw new Error('chunk');
});

vi.mock('@components/components/CodeBlock/highlighter', () => ({
    SyntaxHighlighter: ({ children }: { children: string }) => <pre>{children}</pre>,
}));

vi.mock('@components/components/Toast', () => ({
    toast: {
        success: vi.fn(),
        error: vi.fn(),
    },
}));

describe('CodeBlock format load failure', () => {
    it('does not write code back when the SQL formatter fails to load', async () => {
        const user = userEvent.setup();
        const onChange = vi.fn();

        render(
            <ThemeProvider theme={themeV2}>
                <I18nextProvider i18n={i18n}>
                    <CodeBlock code="select id from orders" language="sql" onChange={onChange} />
                </I18nextProvider>
            </ThemeProvider>,
        );

        await user.click(screen.getByTestId('code-block-format'));

        await waitFor(() => expect(toast.error).toHaveBeenCalled());
        expect(onChange).not.toHaveBeenCalled();
    });
});
