import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { GridList } from '@components/.docs/mdx-components';
import { EmptyState } from '@components/components/EmptyState/EmptyState';

import themes from '@conf/theme/themes';

const meta = {
    title: 'Components / EmptyState',
    component: EmptyState,
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A component for displaying empty data states with icon, message, and optional actions',
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
        title: {
            description: 'Primary heading text',
            control: { type: 'text' },
        },
        description: {
            description: 'Optional secondary description text',
            control: { type: 'text' },
        },
        icon: {
            description: 'Phosphor icon name',
            control: { type: 'text' },
        },
        size: {
            description: 'Size variant',
            control: { type: 'select' },
            options: ['sm', 'default', 'lg'],
        },
        action: { table: { disable: true } },
        secondaryAction: { table: { disable: true } },
        image: { table: { disable: true } },
    },
    args: {
        title: 'No Results Found',
        description: 'Try adjusting your search or filters.',
        icon: 'MagnifyingGlass',
        size: 'default',
    },
} satisfies Meta<typeof EmptyState>;

export default meta;

interface SandboxArgs {
    title: string;
    description: string;
    icon: string;
    size: 'sm' | 'default' | 'lg';
    showAction: boolean;
    showSecondaryAction: boolean;
}

export const sandbox: StoryObj<SandboxArgs> = {
    tags: ['dev'],
    argTypes: {
        showAction: {
            description: 'Show primary action button',
            control: { type: 'boolean' },
        },
        showSecondaryAction: {
            description: 'Show secondary action button',
            control: { type: 'boolean' },
        },
    },
    args: {
        showAction: true,
        showSecondaryAction: false,
    },
    render: ({ showAction, showSecondaryAction, ...props }) => (
        <EmptyState
            {...props}
            action={
                showAction
                    ? { label: 'Take action', onClick: () => {}, icon: { icon: 'Plus', source: 'phosphor' } }
                    : undefined
            }
            secondaryAction={
                showSecondaryAction ? { label: 'Learn more', onClick: () => {}, variant: 'text' } : undefined
            }
        />
    ),
};

export const sizes = () => (
    <GridList>
        <EmptyState size="sm" icon="Key" title="Small" description="Compact empty state" />
        <EmptyState size="default" icon="Key" title="Default" description="Standard empty state" />
        <EmptyState size="lg" icon="Key" title="Large" description="Spacious empty state" />
    </GridList>
);

export const withAction = () => (
    <EmptyState
        icon="Key"
        title="No Access Tokens"
        description="Generate a new token to get started."
        action={{
            label: 'Generate new token',
            onClick: () => {},
            icon: { icon: 'Plus', source: 'phosphor' },
        }}
    />
);

export const withTwoActions = () => (
    <EmptyState
        icon="FolderOpen"
        title="No Datasets"
        description="Import datasets or create one manually."
        action={{
            label: 'Import',
            onClick: () => {},
            icon: { icon: 'DownloadSimple', source: 'phosphor' },
        }}
        secondaryAction={{
            label: 'Learn more',
            onClick: () => {},
        }}
    />
);

export const filteredEmpty = () => (
    <EmptyState
        icon="FunnelSimple"
        title="No Access Tokens Found"
        description="No tokens match the current filters."
        action={{
            label: 'Clear filters',
            onClick: () => {},
            icon: { icon: 'X', source: 'phosphor' },
            variant: 'secondary',
        }}
    />
);

export const noPermission = () => (
    <EmptyState icon="Lock" title="No Access" description="You don't have permission to manage access tokens." />
);

export const iconVariants = () => (
    <GridList>
        <EmptyState icon="MagnifyingGlass" title="No Results" description="Try a different search." size="sm" />
        <EmptyState icon="Robot" title="No Service Accounts" description="Create one to get started." size="sm" />
        <EmptyState icon="ChartBar" title="No Data" description="Data will appear here." size="sm" />
        <EmptyState icon="Bell" title="No Notifications" description="You're all caught up." size="sm" />
    </GridList>
);
