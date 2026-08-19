import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';

import { CodeBlockLanguageOption, CodeBlockProps } from '@components/components/CodeBlock/types';
import { Pill } from '@components/components/Pills';

import { CodeBlock, codeBlockDefaults } from '.';

const SAMPLE_SQL = `SELECT
  order_id,
  SUM(amount) AS total_revenue
FROM analytics.orders
WHERE status = 'completed'
GROUP BY 1
ORDER BY total_revenue DESC`;

const SAMPLE_SNOWFLAKE_SQL = `SELECT
  order_id,
  SUM(amount) AS total_revenue
FROM analytics.orders
WHERE status = 'completed'
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
GROUP BY 1
ORDER BY total_revenue DESC`;

const TWO_LANGUAGE_OPTIONS: CodeBlockLanguageOption[] = [
    { value: 'ansi', label: 'ANSI SQL' },
    { value: 'snowflake', label: 'Snowflake' },
];

const MANY_LANGUAGE_OPTIONS: CodeBlockLanguageOption[] = [
    { value: 'ansi', label: 'ANSI SQL' },
    { value: 'snowflake', label: 'Snowflake' },
    { value: 'databricks', label: 'Databricks' },
    { value: 'bigquery', label: 'BigQuery' },
];

const meta = {
    title: 'Forms / CodeBlock',
    component: CodeBlock,
    parameters: {
        layout: 'padded',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle:
                'Read-only syntax-highlighted code. Language is static text when fixed; alchemy TabButtons for 2 options; SimpleSelect for 3+.',
        },
    },
    argTypes: {
        code: {
            description: 'Source code to display.',
            control: { type: 'text' },
        },
        language: {
            description: 'Prism language id used for highlighting.',
            table: { defaultValue: { summary: codeBlockDefaults.language } },
            control: { type: 'text' },
        },
        variant: {
            description: 'Visual chrome: card (bordered) or embedded (transparent).',
            options: ['card', 'embedded'],
            table: { defaultValue: { summary: codeBlockDefaults.variant } },
            control: { type: 'radio' },
        },
        showHeader: {
            table: { defaultValue: { summary: String(codeBlockDefaults.showHeader) } },
            control: { type: 'boolean' },
        },
        showCopy: {
            table: { defaultValue: { summary: String(codeBlockDefaults.showCopy) } },
            control: { type: 'boolean' },
        },
        showLineNumbers: {
            table: { defaultValue: { summary: String(codeBlockDefaults.showLineNumbers) } },
            control: { type: 'boolean' },
        },
        wrap: {
            table: { defaultValue: { summary: String(codeBlockDefaults.wrap) } },
            control: { type: 'boolean' },
        },
    },
    args: {
        code: SAMPLE_SQL,
        language: 'sql',
        showHeader: true,
        showCopy: true,
        wrap: true,
    },
} satisfies Meta<typeof CodeBlock>;

export default meta;

type Story = StoryObj<typeof meta>;

function StatefulLanguageCodeBlock({
    languageOptions,
    initialLanguage = languageOptions[0]?.value,
    codesByLanguage,
    ...rest
}: Omit<CodeBlockProps, 'selectedLanguage' | 'onLanguageChange' | 'code'> & {
    languageOptions: CodeBlockLanguageOption[];
    initialLanguage?: string;
    codesByLanguage?: Record<string, string>;
}) {
    const [selectedLanguage, setSelectedLanguage] = useState(initialLanguage);
    const code = (selectedLanguage && codesByLanguage?.[selectedLanguage]) || SAMPLE_SQL;

    return (
        <CodeBlock
            {...rest}
            code={code}
            language="sql"
            languageOptions={languageOptions}
            selectedLanguage={selectedLanguage}
            onLanguageChange={setSelectedLanguage}
        />
    );
}

export const Default: Story = {};

export const WithLineNumbers: Story = {
    args: {
        showLineNumbers: true,
    },
};

export const StaticLanguageLabel: Story = {
    name: 'Static language (not changeable)',
    args: {
        languageLabel: 'ANSI SQL',
    },
};

export const WithHeaderSlot: Story = {
    name: 'Language label + platform pill',
    args: {
        languageLabel: 'ANSI SQL',
        headerLeft: <Pill label="Snowflake" color="primary" size="sm" clickable={false} />,
    },
};

export const WithLanguageTabs: Story = {
    name: 'Language tabs (2 options)',
    render: (args) => (
        <StatefulLanguageCodeBlock
            {...args}
            languageOptions={TWO_LANGUAGE_OPTIONS}
            codesByLanguage={{
                ansi: SAMPLE_SQL,
                snowflake: SAMPLE_SNOWFLAKE_SQL,
            }}
        />
    ),
};

export const WithLanguageSelect: Story = {
    name: 'Language select (3+ options)',
    render: (args) => (
        <StatefulLanguageCodeBlock {...args} languageOptions={MANY_LANGUAGE_OPTIONS} initialLanguage="ansi" />
    ),
};

export const Embedded: Story = {
    args: {
        variant: 'embedded',
        showHeader: false,
        showCopy: false,
        showLineNumbers: true,
    },
};

export const Truncated: Story = {
    args: {
        isTruncated: true,
    },
};

export const Yaml: Story = {
    args: {
        language: 'yaml',
        showLineNumbers: true,
        code: `name: orders_revenue
type: metric
sql: |
  SELECT SUM(amount) FROM orders`,
    },
};
