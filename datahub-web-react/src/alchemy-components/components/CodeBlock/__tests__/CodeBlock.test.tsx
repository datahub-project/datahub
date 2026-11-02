import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React, { useState } from 'react';
import { I18nextProvider } from 'react-i18next';
import { ThemeProvider } from 'styled-components';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { CodeBlock } from '@components/components/CodeBlock/CodeBlock';
import { type FormatCodeResult, formatCode } from '@components/components/CodeBlock/formatCode';
import { toast } from '@components/components/Toast';

import themeV2 from '@conf/theme/themeV2';
import i18n from '@src/i18n/i18n';

vi.mock('@components/components/CodeBlock/formatCode', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@components/components/CodeBlock/formatCode')>();
    return { ...actual, formatCode: vi.fn(actual.formatCode) };
});

vi.mock('@components/components/Toast', () => ({
    toast: {
        success: vi.fn(),
        error: vi.fn(),
    },
}));

const originalClipboard = navigator.clipboard;
const formatCodeMock = vi.mocked(formatCode);

function heldFormatResult() {
    let release: (result: FormatCodeResult) => void = () => undefined;
    const pending = new Promise<FormatCodeResult>((resolve) => {
        release = resolve;
    });
    return { pending, release: (result: FormatCodeResult) => release(result) };
}

function EditableSql({ onChange }: { onChange: (value: string) => void }) {
    const [code, setCode] = useState('select id from orders');
    return (
        <>
            <button type="button" onClick={() => setCode('select id from customers')}>
                Edit
            </button>
            <CodeBlock code={code} language="sql" onChange={onChange} />
        </>
    );
}

function content(): HTMLElement {
    return screen.getByTestId('code-block-content');
}

function renderCodeBlock(ui: React.ReactElement) {
    return render(
        <ThemeProvider theme={themeV2}>
            <I18nextProvider i18n={i18n}>{ui}</I18nextProvider>
        </ThemeProvider>,
    );
}

describe('CodeBlock', () => {
    afterEach(() => {
        Object.defineProperty(navigator, 'clipboard', {
            configurable: true,
            value: originalClipboard,
        });
        formatCodeMock.mockClear();
        vi.mocked(toast.error).mockClear();
    });

    it('renders code content', () => {
        renderCodeBlock(<CodeBlock code="SELECT 1" language="sql" />);

        expect(content()).toHaveTextContent('SELECT 1');
    });

    it.each([
        ['sql', 'SELECT id FROM orders'],
        ['snowflake', 'SELECT id FROM orders'],
        ['yml', 'name: orders'],
        ['gql', 'query { user { name } }'],
        ['py', 'def load():\n    return 1'],
        ['protobuf', 'message User { string name = 1; }'],
    ])('highlights %s with an eagerly registered grammar', (language, code) => {
        renderCodeBlock(<CodeBlock code={code} language={language} />);

        expect(content().querySelector('.token')).not.toBeNull();
    });

    it('loads an uncommon grammar on demand and then highlights it', async () => {
        renderCodeBlock(<CodeBlock code="object Main { val total = 1 }" language="scala" />);

        expect(content()).toHaveTextContent('object Main');
        expect(content().querySelector('.token')).toBeNull();
        await waitFor(() => expect(content().querySelector('.token')).not.toBeNull());
    });

    it('shows an unknown language as plain text', () => {
        renderCodeBlock(<CodeBlock code="hello world" language="not-a-language" />);

        expect(content()).toHaveTextContent('hello world');
        expect(content().querySelector('.token')).toBeNull();
    });

    it('shows static language label when not changeable', () => {
        renderCodeBlock(<CodeBlock code="SELECT 1" language="sql" showHeader showCopy />);

        expect(screen.getByText('SQL')).toBeInTheDocument();
        expect(screen.getByTestId('code-block-copy')).toHaveTextContent('Copy');
    });

    it('renders headerLeft slot without auto language label', () => {
        renderCodeBlock(
            <CodeBlock
                code="SELECT 1"
                language="sql"
                showHeader
                languageLabel={false}
                headerLeft={<span>Snowflake</span>}
            />,
        );

        expect(screen.getByText('Snowflake')).toBeInTheDocument();
        expect(screen.queryByText('SQL')).not.toBeInTheDocument();
    });

    it('renders explicit language label to the left of headerLeft', () => {
        renderCodeBlock(
            <CodeBlock
                code="SELECT 1"
                language="sql"
                showHeader
                languageLabel="ANSI SQL"
                headerLeft={<span>Snowflake</span>}
            />,
        );

        const header = screen.getByText('ANSI SQL').parentElement;
        expect(header?.textContent).toMatch(/^ANSI SQLSnowflake/);
    });

    it('renders tab switch for exactly two changeable languages', async () => {
        const user = userEvent.setup();
        const onLanguageChange = vi.fn();

        renderCodeBlock(
            <CodeBlock
                code="SELECT 1"
                language="sql"
                showHeader
                selectedLanguage="ansi"
                languageOptions={[
                    { value: 'ansi', label: 'ANSI SQL' },
                    { value: 'snowflake', label: 'Snowflake' },
                ]}
                onLanguageChange={onLanguageChange}
            />,
        );

        expect(screen.getByRole('tablist')).toBeInTheDocument();
        await user.click(screen.getByRole('tab', { name: 'Snowflake' }));
        expect(onLanguageChange).toHaveBeenCalledWith('snowflake');
    });

    it('renders static text when two options are provided without onLanguageChange', () => {
        renderCodeBlock(
            <CodeBlock
                code="SELECT 1"
                language="sql"
                showHeader
                selectedLanguage="ansi"
                languageOptions={[
                    { value: 'ansi', label: 'ANSI SQL' },
                    { value: 'snowflake', label: 'Snowflake' },
                ]}
            />,
        );

        expect(screen.getByText('ANSI SQL')).toBeInTheDocument();
        expect(screen.queryByRole('tablist')).not.toBeInTheDocument();
    });

    it('should render a writable editor when onChange is provided', async () => {
        const user = userEvent.setup();
        const onChange = vi.fn();

        renderCodeBlock(<CodeBlock code="SELECT 1" language="sql" onChange={onChange} />);

        const editor = screen.getByTestId('code-block-editor');
        expect(editor).toBeInTheDocument();
        expect(screen.getByTestId('code-block-format')).toHaveTextContent('Format');
        await user.type(editor, 'x');
        expect(onChange).toHaveBeenCalled();
    });

    it('should pretty-print JSON from the Format button', async () => {
        const user = userEvent.setup();
        const onChange = vi.fn();

        renderCodeBlock(<CodeBlock code='{"a":1}' language="json" languageLabel="JSON" onChange={onChange} />);

        await user.click(screen.getByTestId('code-block-format'));
        await waitFor(() => expect(onChange).toHaveBeenCalledWith('{\n  "a": 1\n}\n'));
    });

    it('should format SQL from the Format button', async () => {
        const user = userEvent.setup();
        const onChange = vi.fn();

        renderCodeBlock(<CodeBlock code="select id from orders" language="sql" onChange={onChange} />);

        await user.click(screen.getByTestId('code-block-format'));
        await waitFor(() => expect(onChange).toHaveBeenCalledWith('SELECT\n  id\nFROM\n  orders\n'));
    });

    it('ignores repeat Format clicks while a format is in flight', async () => {
        // The loading button disables pointer events; bypass that to exercise the in-flight guard.
        const user = userEvent.setup({ pointerEventsCheck: 0 });
        const held = heldFormatResult();
        formatCodeMock.mockReturnValueOnce(held.pending);

        renderCodeBlock(<CodeBlock code="select id from orders" language="sql" onChange={vi.fn()} />);
        await user.click(screen.getByTestId('code-block-format'));
        await user.click(screen.getByTestId('code-block-format'));

        expect(formatCodeMock).toHaveBeenCalledTimes(1);
        await act(async () => held.release({ formatted: 'SELECT 1' }));
    });

    it('drops a format result when the code changed while it was in flight', async () => {
        const user = userEvent.setup();
        const onChange = vi.fn();
        const held = heldFormatResult();
        formatCodeMock.mockReturnValueOnce(held.pending);

        renderCodeBlock(<EditableSql onChange={onChange} />);
        await user.click(screen.getByTestId('code-block-format'));
        await user.click(screen.getByRole('button', { name: 'Edit' }));
        await act(async () => held.release({ formatted: 'SELECT id FROM orders' }));

        expect(onChange).not.toHaveBeenCalled();
    });

    it('shows an error and keeps the code when the formatter is unavailable', async () => {
        const user = userEvent.setup();
        const onChange = vi.fn();
        formatCodeMock.mockResolvedValueOnce({ error: 'unavailable' });

        renderCodeBlock(<CodeBlock code="select id from orders" language="sql" onChange={onChange} />);
        await user.click(screen.getByTestId('code-block-format'));

        await waitFor(() => expect(toast.error).toHaveBeenCalled());
        expect(onChange).not.toHaveBeenCalled();
    });

    it('should stay read-only when isReadOnly is set even with onChange', () => {
        renderCodeBlock(<CodeBlock code="SELECT 1" language="sql" onChange={() => undefined} isReadOnly />);

        expect(screen.queryByTestId('code-block-editor')).not.toBeInTheDocument();
        expect(screen.queryByTestId('code-block-format')).not.toBeInTheDocument();
        expect(content()).toHaveTextContent('SELECT 1');
    });

    it('should render a parent error message', () => {
        renderCodeBlock(<CodeBlock code="SELECT 1" language="sql" error="Answer is required" />);

        expect(screen.getByTestId('code-block-error')).toHaveTextContent('Answer is required');
        expect(screen.queryByTestId('code-block-warning')).not.toBeInTheDocument();
    });

    it('should render a parent warning message', () => {
        renderCodeBlock(<CodeBlock code="SELECT 1" language="sql" warning="Check this query" />);

        expect(screen.getByTestId('code-block-warning')).toHaveTextContent('Check this query');
    });

    it('should warn about broken SQL when Validate is clicked', async () => {
        const user = userEvent.setup();
        renderCodeBlock(
            <CodeBlock code="SELECT FROM orders" language="sql" onChange={() => undefined} validateSyntax />,
        );

        expect(screen.queryByTestId('code-block-warning')).not.toBeInTheDocument();
        await user.click(screen.getByTestId('code-block-validate'));
        expect(await screen.findByTestId('code-block-warning')).toHaveTextContent(/SELECT is missing columns/i);
    });

    it('renders inline diff marks when diffAgainst differs', () => {
        renderCodeBlock(
            <CodeBlock
                code="COUNT(DISTINCT CASE WHEN status IN ('active','trial')"
                diffAgainst="COUNT(DISTINCT CASE WHEN status = 'active'"
                language="sql"
            />,
        );

        expect(screen.getByTestId('code-block-diff')).toBeInTheDocument();
        expect(screen.getByText("= 'active'", { selector: 'del' })).toBeInTheDocument();
        expect(screen.getByText("IN ('active','trial')", { selector: 'ins' })).toBeInTheDocument();
    });

    it('keeps diff mode read-only when onChange is provided', () => {
        renderCodeBlock(<CodeBlock code="SELECT 2" diffAgainst="SELECT 1" language="sql" onChange={() => undefined} />);

        expect(screen.getByTestId('code-block-diff')).toBeInTheDocument();
        expect(screen.queryByTestId('code-block-editor')).not.toBeInTheDocument();
        expect(screen.queryByTestId('code-block-format')).not.toBeInTheDocument();
    });

    it('copies the displayed version while showing a diff', async () => {
        const user = userEvent.setup();
        const writeText = vi.fn().mockResolvedValue(undefined);
        Object.defineProperty(navigator, 'clipboard', {
            configurable: true,
            value: { writeText },
        });

        renderCodeBlock(<CodeBlock code="SELECT 2" diffAgainst="SELECT 1" language="sql" />);
        await user.click(screen.getByTestId('code-block-copy'));

        expect(writeText).toHaveBeenCalledWith('SELECT 2');
    });

    it('keeps the highlighter when diffAgainst matches code', () => {
        renderCodeBlock(<CodeBlock code="SELECT 1" diffAgainst="SELECT 1" language="sql" />);

        expect(content().querySelector('.token')).not.toBeNull();
        expect(screen.queryByTestId('code-block-diff')).not.toBeInTheDocument();
    });

    it('should list multiple SQL validation issues when Validate is clicked', async () => {
        const user = userEvent.setup();
        renderCodeBlock(
            <CodeBlock
                code={'SELECT FROM orders;\nSELECT * FROM'}
                language="sql"
                onChange={() => undefined}
                validateSyntax
            />,
        );

        await user.click(screen.getByTestId('code-block-validate'));
        const warning = await screen.findByTestId('code-block-warning');
        expect(warning).toHaveTextContent(/SELECT is missing columns/i);
        expect(warning).toHaveTextContent(/FROM is missing a table name/i);
    });
});
