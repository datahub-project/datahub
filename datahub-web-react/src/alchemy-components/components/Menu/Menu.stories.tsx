import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';

import { Menu } from '@components/components/Menu/Menu';

const onClick = (key: string) => console.log('Click on ', key);

// Auto Docs
const meta = {
    title: 'Components / Menu',
    component: Menu,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'Used to show menu',
        },
    },

    // Component-level argTypes
    argTypes: {
        items: {
            description: 'List of menu items. See `ItemType` for details.',
        },
    },

    // Define default args
    args: {
        items: [
            {
                type: 'item',
                key: 'Test simple',
                title: 'Test simple',
                onClick: () => onClick('Test simple'),
            },
            {
                type: 'item',
                key: 'Test full',
                title: 'Test full',
                description: 'description',
                tooltip: 'tooltip',
                icon: 'Globe',
                onClick: () => onClick('Test full'),
            },
            {
                type: 'item',
                key: 'Test disabled',
                title: 'Test disabled',
                description: 'description',
                tooltip: 'tooltip',
                icon: 'Globe',
                disabled: true,
                onClick: () => onClick('Test disabled'),
            },
            {
                type: 'item',
                key: 'Test danger',
                title: 'Test danger',
                description: 'description',
                tooltip: 'tooltip',
                icon: 'Globe',
                danger: true,
                onClick: () => onClick('Test danger'),
            },
            {
                type: 'item',
                key: 'Test disabled danger',
                title: 'Test disabled danger',
                description: 'description',
                tooltip: 'tooltip',
                icon: 'Globe',
                disabled: true,
                danger: true,
                onClick: () => onClick('Test disabled danger'),
            },
            {
                type: 'group',
                key: 'Test group',
                title: 'Test group',
                children: [
                    {
                        type: 'item',
                        key: 'Test in group',
                        title: 'Test in group',
                        description: 'description',
                        tooltip: 'tooltip',
                        icon: 'Globe',
                        onClick: () => onClick('Test in group'),
                    },
                ],
            },
            {
                type: 'divider',
                key: 'Test divider',
            },
            {
                type: 'item',
                key: 'Test with children',
                title: 'Test with children',
                description: 'description',
                icon: 'Globe',
                children: [
                    {
                        type: 'item',
                        key: 'Test child',
                        title: 'Test child',
                        description: 'description',
                        tooltip: 'tooltip',
                        icon: 'Globe',
                        onClick: () => onClick('Test child'),
                    },
                ],
            },
        ],
    },
} satisfies Meta<typeof Menu>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => (
        <Menu {...props}>
            <button type="button">Click</button>
        </Menu>
    ),
};
