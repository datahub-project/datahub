// Disabling no hardcoded colors rule in stories files
/* eslint-disable rulesdir/no-hardcoded-colors */
import { Meta, StoryObj } from '@storybook/react';

import { Bar } from '@components/components/Bar/Bar';

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
                defaultValue: { summary: '#533FD1' },
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
        color: '#533FD1',
        coloredBars: 2,
        size: 'default',
    },
};

export default meta;

type Story = StoryObj<typeof meta>;

// Sandbox Story
export const sandbox: Story = {};

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
        color: '#533FD1',
    },
};

export const noColoredBars: Story = {
    args: {
        size: 'default',
        coloredBars: 0,
        color: '#C6C0E0',
    },
};
