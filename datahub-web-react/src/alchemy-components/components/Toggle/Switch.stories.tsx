import React from 'react';
import type { Meta, StoryObj } from '@storybook/react';
import { GridList } from '@components/.docs/mdx-components';
import { Switch, switchDefaults } from './Switch';
import { AVAILABLE_ICONS } from '../Icon';

const meta = {
    title: 'Forms / Switch',
    component: Switch,
    // Display Properties
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'Switch is a component that is used to get user input in the state of a toggle.',
        },
    },
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
        isIntermediate: {
            description: '',
            table: {
                defaultValue: { summary: switchDefaults?.isIntermediate?.toString() },
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
    args: {
        label: switchDefaults.label,
        icon: switchDefaults.icon,
        colorScheme: switchDefaults.colorScheme,
        size: switchDefaults.size,
        isSquare: switchDefaults.isSquare,
        isChecked: switchDefaults.isChecked,
        isDisabled: switchDefaults.isDisabled,
        isIntermediate: switchDefaults.isIntermediate,
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
    <GridList>
        <Switch label="Small Switch" size="sm" />
        <Switch label="Medium Switch" size="md" />
        <Switch label="Large Switch" size="lg" />
        <Switch label="XLarge Switch" size="xl" />
    </GridList>
);

export const colors = () => (
    <GridList>
        <Switch label="Default Switch" isChecked colorScheme="violet" />
        <Switch label="Green Switch" isChecked colorScheme="green" />
        <Switch label="Red Switch" isChecked colorScheme="red" />
        <Switch label="Blue Switch" isChecked colorScheme="blue" />
        <Switch label="Gray Switch" isChecked colorScheme="gray" />
    </GridList>
);

export const states = () => (
    <GridList>
        <Switch label="Disabled Switch" isDisabled />
        <Switch label="Required Switch" isRequired />
        <Switch label="Switch with Icon" icon="Add" />
    </GridList>
);

export const types = () => (
    <GridList width="400px">
        <Switch label="Default Switch" />
        <Switch label="Square Switch" isSquare />
    </GridList>
);
