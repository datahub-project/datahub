import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import styled, { ThemeProvider } from 'styled-components';

import { Breadcrumb } from '@components/components/Breadcrumb';
import { breadcrumbDefaults } from '@components/components/Breadcrumb/defaults';
import { Icon } from '@components/components/Icon';

import themes from '@conf/theme/themes';

// Auto Docs
const meta = {
    title: 'Components / Breadcrumb',
    component: Breadcrumb,

    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'Breadcrumb navigation component',
        },
    },

    argTypes: {
        items: {
            description:
                'The breadcrumb items. Items can be a link (`href`), clickable (`onClick`), or non-interactive text.',
            control: { type: 'object' },
        },
        showPopover: {
            description: 'Whether to show breadcrumbs in popover when they are truncated',
            table: {
                defaultValue: { summary: `${breadcrumbDefaults.showPopover}` },
            },
            control: {
                type: 'boolean',
            },
        },
    },

    args: {
        items: [
            { key: 'Home', label: 'Home', href: '/' },
            { key: 'Data Sources', label: 'Data Sources', href: '/data-sources' },
            { key: 'Details', label: 'Details', isActive: true },
        ],
    },
    decorators: [
        (Story) => {
            return (
                <MemoryRouter>
                    <Story />
                </MemoryRouter>
            );
        },
        (Story) => {
            return (
                <ThemeProvider theme={themes.themeV2}>
                    <Story />
                </ThemeProvider>
            );
        },
    ],
} satisfies Meta<typeof Breadcrumb>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Breadcrumb {...props} />,
};

export const basic = () => (
    <Breadcrumb
        items={[
            { key: 'Home', label: 'Home', href: '/' },
            { key: 'Library', label: 'Library', href: '/library' },
            { key: 'Details', label: 'Details', isActive: true },
        ]}
    />
);

export const withClickableButton = () => (
    <Breadcrumb
        items={[
            { key: 'Back', label: 'Back', onClick: () => {} },
            { key: 'Data Sources', label: 'Data Sources', href: '/sources' },
            { key: 'Details', label: 'Details', isActive: true },
        ]}
    />
);

export const onlyText = () => (
    <Breadcrumb
        items={[
            { key: 'level1', label: 'Level 1' },
            { key: 'level2', label: 'Level 2' },
            { key: 'level3', label: 'Final Level', isActive: true },
        ]}
    />
);

export const withCustomSeparator = () => (
    <Breadcrumb
        items={[
            {
                key: 'Home',
                label: 'Home',
                href: '/',
                separator: <Icon icon="ArrowRight" source="phosphor" color="gray" colorLevel={1800} size="sm" />,
            },
            {
                key: 'Projects',
                label: 'Projects',
                href: '/projects',
                separator: <Icon icon="ArrowRight" source="phosphor" color="gray" colorLevel={1800} size="sm" />,
            },
            { key: 'Overview', label: 'Overview', isActive: true },
        ]}
    />
);

export const longBreadcrumb = () => (
    <Breadcrumb
        items={[
            { key: 'Home', label: 'Home', href: '/' },
            { key: 'Section', label: 'Section', href: '/section' },
            { key: 'Category', label: 'Category', href: '/category' },
            { key: 'Type', label: 'Type', href: '/type' },
            { key: 'Item', label: 'Item', href: '/item' },
            { key: 'Details', label: 'Details', isActive: true },
        ]}
    />
);

const ContainerWithLimitedWidth = styled.div`
    width: 300px;
`;

export const longBreadcrumbWithTruncation = () => {
    return (
        <ContainerWithLimitedWidth>
            <Breadcrumb
                items={[
                    { key: 'Home', label: 'Home', href: '/' },
                    { key: 'Section', label: 'Section', href: '/section' },
                    { key: 'Category', label: 'Category', href: '/category' },
                    { key: 'Type', label: 'Type', href: '/type' },
                    { key: 'Item', label: 'Item', href: '/item' },
                    { key: 'Button Example', label: 'Button Example', onClick: () => {} },
                    { key: 'Details', label: 'Details', isActive: true },
                ]}
            />
        </ContainerWithLimitedWidth>
    );
};
