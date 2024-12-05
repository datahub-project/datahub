import React from 'react';

import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';

import { GridList } from '@components/.docs/mdx-components';
import { AVAILABLE_ICONS } from '../Icon';
import { Pill, pillDefault } from './Pill';

const meta = {
    title: 'Components / Pill',
    component: Pill,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A component that is used to get pill',
        },
    },

    // Component-level argTypes
    argTypes: {
        label: {
            description: 'Label for the Pill.',
            table: {
                defaultValue: { summary: pillDefault.label },
            },
            control: {
                type: 'text',
            },
        },
        leftIcon: {
            description: 'The icon to display in the Pill icon.',
            type: 'string',
            options: AVAILABLE_ICONS,
            control: {
                type: 'select',
            },
        },
        rightIcon: {
            description: 'The icon to display in the Pill icon.',
            type: 'string',
            options: AVAILABLE_ICONS,
            control: {
                type: 'select',
            },
        },
        size: {
            description: 'The size of the pill.',
            options: ['sm', 'md', 'lg', 'xl'],
            table: {
                defaultValue: { summary: pillDefault.size },
            },
            control: {
                type: 'select',
            },
        },
        variant: {
            description: 'The size of the Pill.',
            options: ['filled', 'outline'],
            table: {
                defaultValue: { summary: pillDefault.variant },
            },
            control: {
                type: 'select',
            },
        },
        colorScheme: {
            description: 'The color of the Pill.',
            options: ['violet', 'green', 'red', 'blue', 'gray'],
            table: {
                defaultValue: { summary: pillDefault.color },
            },
            control: {
                type: 'select',
            },
        },
    },

    // Define defaults
    args: {
        label: pillDefault.label,
        leftIcon: pillDefault.leftIcon,
        rightIcon: pillDefault.rightIcon,
        size: pillDefault.size,
        variant: pillDefault.variant,
    },
} satisfies Meta<typeof Pill>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Pill {...props} />,
};

export const sizes = () => (
    <GridList>
        <Pill label="Default" />
        <Pill label="small" size="sm" />
        <Pill label="large" size="lg" />
    </GridList>
);

export const colors = () => (
    <GridList>
        <Pill label="Default" />
        <Pill label="Violet" colorScheme="violet" />
        <Pill label="Green" colorScheme="green" />
        <Pill label="Red" colorScheme="red" />
        <Pill label="Blue" colorScheme="blue" />
        <Pill label="Gray" colorScheme="gray" />
    </GridList>
);

export const withIcon = () => (
    <GridList>
        <Pill label="left icon" leftIcon="AutoMode" />
        <Pill label="right icon" rightIcon="Close" />
        <Pill label="both icon" leftIcon="AutoMode" rightIcon="Close" />
    </GridList>
);
