import React from 'react';
import { Meta, StoryObj } from '@storybook/react';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { Bar } from './Bar';

const meta: Meta<typeof Bar> = {
    title: 'Components / Bar',
    component: Bar,

    // Component-level parameters
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'Displays a bar component with up to three segments that can be dynamically colored.',
        },
    },

    // Component-level argTypes
    argTypes: {
        color: {
            description: 'Color of the filled bars.',
            control: 'color',
            table: {
                defaultValue: { summary: colors.violet[500] },
            },
        },
        coloredBars: {
            description: 'Number of bars to color.',
            control: { type: 'number', min: 0, max: 3 },
            table: {
                defaultValue: { summary: '2' },
            },
        },
        size: {
            description: 'Size of the bars.',
            control: 'select',
            options: ['sm', 'lg', 'default'],
            table: {
                defaultValue: { summary: 'default' },
            },
        },
    },

    // Default props
    args: {
        color: colors.violet[500],
        coloredBars: 2,
        size: 'default',
    },
};

export default meta;

type Story = StoryObj<typeof meta>;

// Sandbox Story
export const sandbox: Story = {
    render: (props) => <Bar {...props} />,
};

// Example Stories
export const withCustomColors: Story = {
    args: {
        color: '#ff6b6b',
        coloredBars: 3,
    },
};

export const smallBars: Story = {
    args: {
        size: 'sm',
        coloredBars: 1,
        color: '#6bc1ff',
    },
};

export const defaultBars: Story = {
    args: {
        size: 'default',
        coloredBars: 2,
        color: colors.violet[500],
    },
};

export const noColoredBars: Story = {
    args: {
        size: 'default',
        coloredBars: 0,
        color: '#C6C0E0',
    },
};
