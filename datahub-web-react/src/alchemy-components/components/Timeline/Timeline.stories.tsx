import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import { Timeline } from './Timeline';

// Auto Docs
const meta = {
    title: 'Components / Timeline',
    component: Timeline,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'Vertical display timeline.',
        },
    },

    // Component-level argTypes
    argTypes: {
        items: {
            description: 'A list of items to render in the timeline',
        },
        renderContent: {
            description: "A function to render a content of the timeline's item",
        },
        renderDot: {
            description: "A function to render a dot of the timeline's item",
        },
    },

    // Define default args
    args: {
        items: [
            {
                key: '1',
            },
            {
                key: '2',
            },
            {
                key: '3',
            },
        ],
        renderContent: (item) => `Content for item ${item.key}`,
        renderDot: (item) => {
            if (item.key === '2') {
                return <div style={{ width: '10px', height: '10px', background: 'red' }} />;
            }
            return undefined;
        },
    },
} satisfies Meta<typeof Timeline>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => {
        return <Timeline {...props} />;
    },
};
