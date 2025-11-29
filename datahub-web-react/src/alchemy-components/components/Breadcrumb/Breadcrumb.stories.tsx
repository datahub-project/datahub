import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { Breadcrumb } from '@components/components/Breadcrumb';
import { Icon } from '@components/components/Icon';

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
    },

    args: {
        items: [
            { label: 'Home', href: '/' },
            { label: 'Data Sources', href: '/data-sources' },
            { label: 'Details', isCurrent: true },
        ],
    },
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
            { label: 'Home', href: '/' },
            { label: 'Library', href: '/library' },
            { label: 'Details', isCurrent: true },
        ]}
    />
);

export const withClickableButton = () => (
    <Breadcrumb
        items={[
            { label: 'Back', onClick: () => {} },
            { label: 'Data Sources', href: '/sources' },
            { label: 'Details', isCurrent: true },
        ]}
    />
);

export const onlyText = () => (
    <Breadcrumb items={[{ label: 'Level 1' }, { label: 'Level 2' }, { label: 'Final Level', isCurrent: true }]} />
);

export const withCustomSeparator = () => (
    <Breadcrumb
        items={[
            {
                label: 'Home',
                href: '/',
                separator: <Icon icon="ArrowRight" source="phosphor" color="gray" colorLevel={1800} size="sm" />,
            },
            {
                label: 'Projects',
                href: '/projects',
                separator: <Icon icon="ArrowRight" source="phosphor" color="gray" colorLevel={1800} size="sm" />,
            },
            { label: 'Overview', isCurrent: true },
        ]}
    />
);

export const longBreadcrumb = () => (
    <Breadcrumb
        items={[
            { label: 'Home', href: '/' },
            { label: 'Section', href: '/section' },
            { label: 'Category', href: '/category' },
            { label: 'Type', href: '/type' },
            { label: 'Item', href: '/item' },
            { label: 'Details', isCurrent: true },
        ]}
    />
);
