import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React, { useState } from 'react';
import { I18nextProvider } from 'react-i18next';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { CodeBlock } from '@components/components/CodeBlock/CodeBlock';

import themeV2 from '@conf/theme/themeV2';
import i18n from '@src/i18n/i18n';

const heldImport = vi.hoisted(() => {
    let releaseImport: () => void = () => undefined;
    const pending = new Promise<void>((resolve) => {
        releaseImport = resolve;
    });
    return {
        pending,
        releaseImport: () => releaseImport(),
    };
});

vi.mock('sql-formatter', async () => {
    await heldImport.pending;
    return {
        format: (query: string) => query.toUpperCase(),
    };
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

function Editable({ onChange }: { onChange: (value: string) => void }) {
    const [code, setCode] = useState('select id from orders');
    return (
        <ThemeProvider theme={themeV2}>
            <I18nextProvider i18n={i18n}>
                <button type="button" onClick={() => setCode('select id from customers')}>
                    Edit
                </button>
                <CodeBlock code={code} language="sql" onChange={onChange} />
            </I18nextProvider>
        </ThemeProvider>
    );
}

describe('CodeBlock format races', () => {
    it('drops a SQL format result when the text changed while the formatter was loading', async () => {
        const user = userEvent.setup();
        const onChange = vi.fn();
        render(<Editable onChange={onChange} />);

        await user.click(screen.getByTestId('code-block-format'));
        await user.click(screen.getByRole('button', { name: 'Edit' }));
        await act(async () => {
            heldImport.releaseImport();
            await heldImport.pending;
        });

        expect(onChange).not.toHaveBeenCalled();
    });
});
