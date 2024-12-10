import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';

import { GridList } from '@components/.docs/mdx-components';

import { Switch, switchDefaults } from './Switch';
import { AVAILABLE_ICONS } from '../Icon';

const meta = {
    title: 'Forms / Switch',
    component: Switch,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'A component that is used to get user input in the state of a toggle.',
        },
    },

    // Component-level argTypes
    argTypes: {
        label: {
            description: 'Label for the Switch.',
            table: {
                defaultValue: { summary: switchDefaults.label },
            },
            control: {
                type: 'text',
            },
        },
        labelPosition: {
            description: 'The position of the label relative to the Switch.',
            options: ['left', 'top'],
            table: {
                defaultValue: { summary: switchDefaults.labelPosition },
            },
            control: {
                type: 'select',
            },
        },
        icon: {
            description: 'The icon to display in the Switch Slider.',
            type: 'string',
            options: AVAILABLE_ICONS,
            table: {
                defaultValue: { summary: 'undefined' },
            },
            control: {
                type: 'select',
            },
        },
        colorScheme: {
            description: 'The color of the Switch.',
            options: ['violet', 'green', 'red', 'blue', 'gray'],
            table: {
                defaultValue: { summary: switchDefaults.colorScheme },
            },
            control: {
                type: 'select',
            },
        },
        size: {
            description: 'The size of the Button.',
            options: ['sm', 'md', 'lg', 'xl'],
            table: {
                defaultValue: { summary: switchDefaults.size },
            },
            control: {
                type: 'select',
            },
        },
        isSquare: {
            description: 'Whether the Switch is square in shape.',
            table: {
                defaultValue: { summary: switchDefaults?.isSquare?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isChecked: {
            description: 'Whether the Switch is checked.',
            table: {
                defaultValue: { summary: switchDefaults?.isChecked?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isDisabled: {
            description: 'Whether the Switch is in disabled state.',
            table: {
                defaultValue: { summary: switchDefaults?.isDisabled?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isRequired: {
            description: 'Whether the Switch is a required field.',
            table: {
                defaultValue: { summary: switchDefaults?.isRequired?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
    },

    // Define defaults
    args: {
        label: switchDefaults.label,
        labelPosition: switchDefaults.labelPosition,
        icon: switchDefaults.icon,
        colorScheme: switchDefaults.colorScheme,
        size: switchDefaults.size,
        isSquare: switchDefaults.isSquare,
        isChecked: switchDefaults.isChecked,
        isDisabled: switchDefaults.isDisabled,
        isRequired: switchDefaults.isRequired,
    },
} satisfies Meta<typeof Switch>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Switch {...props} />,
};

export const sizes = () => (
    <GridList isVertical>
        <Switch label="Small" size="sm" />
        <Switch label="Medium" size="md" />
        <Switch label="Large" size="lg" />
        <Switch label="XLarge" size="xl" />
    </GridList>
);

export const colors = () => (
    <GridList isVertical>
        <Switch label="Default" isChecked colorScheme="violet" />
        <Switch label="Green" isChecked colorScheme="green" />
        <Switch label="Red" isChecked colorScheme="red" />
        <Switch label="Blue" isChecked colorScheme="blue" />
        <Switch label="Gray" isChecked colorScheme="gray" />
    </GridList>
);

export const states = () => (
    <GridList isVertical>
        <Switch label="Disabled" isDisabled />
        <Switch label="Required" isRequired />
        <Switch label="with Icon" icon="Add" />
    </GridList>
);

export const types = () => (
    <GridList width="400px">
        <Switch label="Default" />
        <Switch label="Square" isSquare />
    </GridList>
);
