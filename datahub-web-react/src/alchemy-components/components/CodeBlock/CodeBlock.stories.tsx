import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';

import { resolveCodeLanguage } from '@components/components/CodeBlock/formatCode';
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

const SAMPLE_YAML = `name: orders_revenue
type: metric
sql: |
  SELECT SUM(amount) FROM orders`;

const LONG_SQL = Array.from({ length: 8 }, () => SAMPLE_SQL).join('\n\n');

const MESSY_SQL = `SELECT    order_id,   SUM(amount) AS total_revenue   
FROM analytics.orders


WHERE status = 'completed'    
GROUP BY 1
ORDER BY total_revenue DESC`;

const MESSY_YAML = `name:    orders_revenue
type:   metric


sql: |
  SELECT SUM(amount) FROM orders
tags: [finance,  prod]`;

const MESSY_JSON = '{"name":"orders_revenue","type":"metric","tags":["finance","prod"]}';

const MESSY_GRAPHQL = 'type Query { user(id: ID!): User }  type User { name:String  email: String }';

const MIXED_LANGUAGE_OPTIONS: CodeBlockLanguageOption[] = [
    { value: 'sql', label: 'SQL' },
    { value: 'yaml', label: 'YAML' },
    { value: 'json', label: 'JSON' },
    { value: 'graphql', label: 'GraphQL' },
];

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
                'Syntax-highlighted code. Read-only by default; pass `onChange` to type, indent with Tab, and format SQL, YAML, JSON, and GraphQL.',
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
            description: 'Soft-wrap long lines.',
            table: { defaultValue: { summary: String(codeBlockDefaults.wrap) } },
            control: { type: 'boolean' },
        },
        maxHeight: {
            description:
                'Max height of the code body. Writable editors default to 400px and scroll inside; pass `"none"` to grow with content.',
            control: { type: 'text' },
        },
        showFormat: {
            description:
                'Show Format in the header when the editor is writable (pretty-prints SQL, YAML, JSON, and GraphQL).',
            table: { defaultValue: { summary: String(codeBlockDefaults.showFormat) } },
            control: { type: 'boolean' },
        },
        validateSyntax: {
            description:
                'Show a Validate action in the header. Clicking lists syntax problems (SQL soft warning; JSON / YAML / GraphQL error).',
            table: { defaultValue: { summary: 'false' } },
            control: { type: 'boolean' },
        },
        error: {
            description: 'Parent hard validation message (invalid border).',
            control: { type: 'text' },
        },
        warning: {
            description: 'Parent soft validation message (warning border).',
            control: { type: 'text' },
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

function StatefulCodeBlock({
    initialCode,
    ...rest
}: Omit<CodeBlockProps, 'code' | 'onChange'> & { initialCode?: string }) {
    const [code, setCode] = useState(initialCode ?? SAMPLE_SQL);

    return <CodeBlock {...rest} code={code} onChange={setCode} />;
}

function StatefulLanguageCodeBlock({
    languageOptions,
    initialLanguage = languageOptions[0]?.value,
    codesByLanguage,
    ...rest
}: Omit<CodeBlockProps, 'selectedLanguage' | 'onLanguageChange' | 'code' | 'onChange'> & {
    languageOptions: CodeBlockLanguageOption[];
    initialLanguage?: string;
    codesByLanguage?: Record<string, string>;
}) {
    const [selectedLanguage, setSelectedLanguage] = useState(initialLanguage);
    const [codes, setCodes] = useState<Record<string, string>>(() => {
        const next: Record<string, string> = {};
        languageOptions.forEach((option) => {
            next[option.value] = codesByLanguage?.[option.value] ?? SAMPLE_SQL;
        });
        return next;
    });
    const code = (selectedLanguage && codes[selectedLanguage]) || SAMPLE_SQL;

    return (
        <CodeBlock
            {...rest}
            code={code}
            language={resolveCodeLanguage(selectedLanguage ?? 'sql')}
            languageOptions={languageOptions}
            selectedLanguage={selectedLanguage}
            onLanguageChange={setSelectedLanguage}
            onChange={(next) => {
                if (!selectedLanguage) {
                    return;
                }
                setCodes((prev) => ({ ...prev, [selectedLanguage]: next }));
            }}
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
        code: SAMPLE_YAML,
    },
};

export const Editable: Story = {
    name: 'Editable SQL',
    render: (args) => (
        <StatefulCodeBlock {...args} initialCode={SAMPLE_SQL} language="sql" placeholder="Write a SQL query…" wrap />
    ),
};

export const EditableWithValidation: Story = {
    name: 'Editable SQL with Validate button',
    render: (args) => (
        <StatefulCodeBlock
            {...args}
            initialCode={'SELECT FROM orders;\nSELECT * FROM'}
            language="sql"
            validateSyntax
            placeholder="Write a SQL query…"
            wrap
        />
    ),
};

export const EditableLongSql: Story = {
    name: 'Editable SQL (scrolls at 400px)',
    render: (args) => <StatefulCodeBlock {...args} initialCode={LONG_SQL} language="sql" wrap />,
};

export const EditableYaml: Story = {
    name: 'Editable YAML (Format pretty-prints)',
    render: (args) => (
        <StatefulCodeBlock {...args} initialCode={MESSY_YAML} language="yaml" languageLabel="YAML" wrap />
    ),
};

export const EditableJson: Story = {
    name: 'Editable JSON (Format pretty-prints)',
    render: (args) => (
        <StatefulCodeBlock {...args} initialCode={MESSY_JSON} language="json" languageLabel="JSON" wrap />
    ),
};

export const EditableGraphql: Story = {
    name: 'Editable GraphQL (Format pretty-prints)',
    render: (args) => (
        <StatefulCodeBlock {...args} initialCode={MESSY_GRAPHQL} language="graphql" languageLabel="GraphQL" wrap />
    ),
};

export const EditableLanguages: Story = {
    name: 'Editable SQL / YAML / JSON / GraphQL',
    render: (args) => (
        <StatefulLanguageCodeBlock
            {...args}
            languageOptions={MIXED_LANGUAGE_OPTIONS}
            codesByLanguage={{
                sql: MESSY_SQL,
                yaml: MESSY_YAML,
                json: MESSY_JSON,
                graphql: MESSY_GRAPHQL,
            }}
            wrap
        />
    ),
};
