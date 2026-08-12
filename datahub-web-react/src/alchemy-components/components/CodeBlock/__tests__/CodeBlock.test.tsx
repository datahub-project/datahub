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

vi.mock('react-syntax-highlighter/dist/esm/styles/prism', () => ({
    __esModule: true,
    ghcolors: {},
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
});
