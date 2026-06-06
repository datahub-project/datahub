import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { Sparkle } from '@phosphor-icons/react/dist/csr/Sparkle';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { GridList } from '@components/.docs/mdx-components';
import { Alert, VARIANT_BUTTON_COLOR_MAP } from '@components/components/Alert/Alert';
import { AlertVariant } from '@components/components/Alert/types';
import { Button } from '@components/components/Button';

import themes from '@conf/theme/themes';

const VARIANTS: AlertVariant[] = ['success', 'error', 'warning', 'info', 'brand', 'unknown'];

const meta = {
    title: 'Components / Alert',
    component: Alert,

    parameters: {
        layout: 'padded',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle:
                'Inline status banner for surfacing success, error, warning, info, brand, or unknown messages within a page or panel. Colors come from semantic theme tokens — never pass hex values.',
        },
    },

    decorators: [
        (Story) => (
            <ThemeProvider theme={themes.themeV2}>
                <Story />
            </ThemeProvider>
        ),
    ],

    argTypes: {
        variant: {
            description: 'Visual style determining colors and the default icon.',
            options: VARIANTS,
            control: { type: 'select' },
            table: {
                type: { summary: VARIANTS.join(' | ') },
                defaultValue: { summary: 'info' },
            },
        },
        title: {
            description: 'Primary message rendered as bold text.',
            control: { type: 'text' },
            table: { type: { summary: 'string | ReactNode' } },
        },
        description: {
            description: 'Optional secondary message rendered below the title.',
            control: { type: 'text' },
            table: { type: { summary: 'string | ReactNode' } },
        },
        errorMessage: {
            description:
                'Technical error detail rendered in a monospace box below the description. Use for stack traces or raw error strings.',
            control: { type: 'text' },
            table: { type: { summary: 'string' } },
        },
        icon: {
            description: 'Override the default icon for the variant. Pass any ReactNode (typically a Phosphor icon).',
            control: false,
            table: { type: { summary: 'ReactNode' } },
        },
        onClose: {
            description: 'When provided, renders a close button that calls this handler on click.',
            control: false,
            table: { type: { summary: '() => void' } },
        },
        action: {
            description: 'Action element rendered on the right (e.g. a Retry button).',
            control: false,
            table: { type: { summary: 'ReactNode' } },
        },
        actionPlacement: {
            description:
                'Where to render the action element. `inline` (default) puts it under the description; `topRight` puts it next to the close button.',
            options: ['inline', 'topRight'],
            control: { type: 'radio' },
            table: {
                type: { summary: "'inline' | 'topRight'" },
                defaultValue: { summary: 'inline' },
            },
        },
    },

    args: {
        variant: 'info',
        title: 'Alert title',
        description: 'Alert description goes here.',
    },
} satisfies Meta<typeof Alert>;

export default meta;

interface SandboxArgs {
    variant: AlertVariant;
    title: string;
    description: string;
    errorMessage?: string;
    showCloseButton: boolean;
    showAction: boolean;
    actionPlacement: 'inline' | 'topRight';
}

export const sandbox: StoryObj<SandboxArgs> = {
    tags: ['dev'],
    argTypes: {
        showCloseButton: {
            description: 'Toggle the built-in close button on/off (wires up onClose).',
            control: { type: 'boolean' },
        },
        showAction: {
            description: 'Toggle a Retry button rendered in the action slot.',
            control: { type: 'boolean' },
        },
    },
    args: {
        showCloseButton: true,
        showAction: false,
        actionPlacement: 'inline',
    },
    render: ({ showCloseButton, showAction, actionPlacement, ...props }) => (
        <Alert
            {...props}
            actionPlacement={actionPlacement}
            onClose={showCloseButton ? () => {} : undefined}
            action={
                showAction ? (
                    <Button variant="text" color={VARIANT_BUTTON_COLOR_MAP[props.variant]} size="sm">
                        Retry
                    </Button>
                ) : undefined
            }
        />
    ),
};

export const variants = () => (
    <GridList isVertical>
        {VARIANTS.map((variant) => (
            <Alert
                key={variant}
                variant={variant}
                title={`${variant.charAt(0).toUpperCase()}${variant.slice(1)} alert`}
                description="Inline status banner with the matching semantic color tokens."
            />
        ))}
    </GridList>
);

export const titleOnly = () => (
    <GridList isVertical>
        <Alert variant="success" title="Saved successfully" />
        <Alert variant="warning" title="This action cannot be undone" />
    </GridList>
);

export const withErrorMessage = () => (
    <GridList isVertical>
        <Alert
            variant="error"
            title="Backfill failed"
            description="The backfill job encountered an error while collecting historical data."
            errorMessage="TypeError: Cannot read properties of undefined (reading 'rows') at SnowflakeClient.runQuery (snowflake.ts:142:18)"
        />
        <Alert
            variant="warning"
            title="Recipe validation warning"
            description="Your recipe has unresolved references."
            errorMessage="WARN: source.config.platform_instance_map[*] contains 2 platforms not present in the registry"
        />
    </GridList>
);

export const dismissible = () => (
    <Alert
        variant="info"
        title="Heads up"
        description="You can dismiss this banner with the close button."
        onClose={() => {
            /* no-op for the story */
        }}
    />
);

export const withAction = () => (
    <Alert
        variant="error"
        title="Connection lost"
        description="We could not reach the metadata service. Check your network and try again."
        action={
            <Button variant="text" color="red" size="sm">
                Retry
            </Button>
        }
    />
);

export const topRightAction = () => (
    <GridList isVertical>
        <Alert
            variant="info"
            title="Schema sync in progress"
            description="We are pulling the latest schema from the upstream warehouse."
            action={
                <Button variant="text" color="blue" size="sm">
                    View progress
                </Button>
            }
            actionPlacement="topRight"
            onClose={() => {
                /* no-op for the story */
            }}
        />
        <Alert
            variant="warning"
            title="Connection unstable"
            description="Some recent metadata may be stale."
            action={
                <Button variant="text" color="yellow" size="sm">
                    Reconnect
                </Button>
            }
            actionPlacement="topRight"
            onClose={() => {
                /* no-op for the story */
            }}
        />
    </GridList>
);

export const customIcon = () => (
    <Alert
        variant="brand"
        title="New AI features available"
        description="Try the new metadata assistant in the side panel."
        icon={<Sparkle size={20} weight="fill" />}
    />
);

export const kitchenSink = () => (
    <Alert
        variant="error"
        title="Assertion run failed"
        description="The smart assertion encountered an error while executing against the Snowflake warehouse."
        errorMessage="SnowflakeQueryError: SQL compilation error: Object 'PROD.ANALYTICS.ORDERS' does not exist or not authorized."
        action={
            <Button variant="text" color="red" size="sm">
                Retry
            </Button>
        }
        onClose={() => {
            /* no-op for the story */
        }}
    />
);
