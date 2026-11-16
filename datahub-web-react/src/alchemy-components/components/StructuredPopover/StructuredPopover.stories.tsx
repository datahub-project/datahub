import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { GridList } from '@components/.docs/mdx-components';
import { Button } from '@components/components/Button';
import { Pill } from '@components/components/Pills';
import { StructuredPopover } from '@components/components/StructuredPopover';

const SECTIONS = [
    {
        title: 'Owner',
        content: 'Data Platform team',
    },
    {
        title: 'Freshness',
        titleSuffix: <Pill label="Healthy" color="green" size="sm" />,
        content: 'Last updated 2 hours ago',
    },
    {
        title: 'Description',
        content: 'One row per order placed through the storefront, refreshed hourly.',
    },
];

const Header = () => <strong>orders_daily</strong>;

const meta = {
    title: 'Components / StructuredPopover',
    component: StructuredPopover,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE],
        docs: {
            subtitle: 'A popover that lays out a header, title, and titled sections in a consistent format.',
        },
    },

    // Component-level argTypes
    argTypes: {
        title: {
            description: 'Muted text shown above the sections.',
            control: {
                type: 'text',
            },
        },
        sections: {
            description: 'List of sections, each with a title, optional title suffix, and content.',
            control: {
                type: 'object',
            },
        },
        width: {
            description: 'Width of the popover in pixels.',
            control: {
                type: 'number',
            },
        },
        placement: {
            description: 'Where the popover appears relative to its trigger.',
            options: ['top', 'bottom', 'left', 'right'],
            control: {
                type: 'select',
            },
        },
        header: {
            description: 'Component rendered at the top of the popover.',
            control: false,
        },
    },

    // Define defaults
    args: {
        title: 'Dataset details',
        sections: SECTIONS,
        width: 300,
        placement: 'bottom',
    },
} satisfies Meta<typeof StructuredPopover>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <StructuredPopover {...props}>
            <Button variant="outline">Hover me</Button>
        </StructuredPopover>
    ),
};

export const variants = () => (
    <GridList>
        <StructuredPopover sections={SECTIONS} width={300}>
            <Button variant="outline">Sections only</Button>
        </StructuredPopover>
        <StructuredPopover title="Dataset details" sections={SECTIONS} width={300}>
            <Button variant="outline">With title</Button>
        </StructuredPopover>
        <StructuredPopover header={Header} title="Dataset details" sections={SECTIONS} width={300}>
            <Button variant="outline">With header</Button>
        </StructuredPopover>
    </GridList>
);

// With no title and no sections the popover is skipped and only the trigger renders.
export const empty = () => (
    <StructuredPopover sections={[]}>
        <Button variant="outline">No popover</Button>
    </StructuredPopover>
);
