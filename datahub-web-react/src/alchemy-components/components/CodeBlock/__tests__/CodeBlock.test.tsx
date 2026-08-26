import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { I18nextProvider } from 'react-i18next';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { CodeBlock } from '@components/components/CodeBlock/CodeBlock';

import themeV2 from '@conf/theme/themeV2';
import i18n from '@src/i18n/i18n';

vi.mock('react-syntax-highlighter', () => ({
    __esModule: true,
    Prism: ({ children }: { children: string }) => <pre data-testid="mock-highlighter">{children}</pre>,
}));

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

describe('CodeBlock', () => {
    it('renders code content', () => {
        renderCodeBlock(<CodeBlock code="SELECT 1" language="sql" />);

        expect(screen.getByTestId('mock-highlighter')).toHaveTextContent('SELECT 1');
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
        expect(onChange).toHaveBeenCalledWith('{\n  "a": 1\n}\n');
    });

    it('should stay read-only when isReadOnly is set even with onChange', () => {
        renderCodeBlock(<CodeBlock code="SELECT 1" language="sql" onChange={() => undefined} isReadOnly />);

        expect(screen.queryByTestId('code-block-editor')).not.toBeInTheDocument();
        expect(screen.queryByTestId('code-block-format')).not.toBeInTheDocument();
        expect(screen.getByTestId('mock-highlighter')).toHaveTextContent('SELECT 1');
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
