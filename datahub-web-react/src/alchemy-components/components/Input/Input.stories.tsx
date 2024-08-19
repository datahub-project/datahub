import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';

import { GridList } from '@components/.docs/mdx-components';
import { AVAILABLE_ICONS } from '../Icon';

import { Input, inputDefaults } from './Input';

const meta = {
    title: 'Forms / Input',
    component: Input,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'A component that is used to get user input in a single line field.',
        },
    },

    // Component-level argTypes
    argTypes: {
        value: {
            description: 'Value for the Input.',
            table: {
                defaultValue: { summary: inputDefaults.value as string },
            },
            control: {
                type: 'text',
            },
        },
        label: {
            description: 'Label for the Input.',
            table: {
                defaultValue: { summary: inputDefaults.label },
            },
            control: {
                type: 'text',
            },
        },
        placeholder: {
            description: 'Placeholder for the Input.',
            table: {
                defaultValue: { summary: inputDefaults.placeholder },
            },
            control: {
                type: 'text',
            },
        },
        icon: {
            description: 'The icon to display in the Input.',
            type: 'string',
            options: AVAILABLE_ICONS,
            table: {
                defaultValue: { summary: 'undefined' },
            },
            control: {
                type: 'select',
            },
        },
        error: {
            description: 'Enforce error state on the Input.',
            table: {
                defaultValue: { summary: inputDefaults.error },
            },
            control: {
                type: 'text',
            },
        },
        warning: {
            description: 'Enforce warning state on the Input.',
            table: {
                defaultValue: { summary: inputDefaults.warning },
            },
            control: {
                type: 'text',
            },
        },
        isSuccess: {
            description: 'Enforce success state on the Input.',
            table: {
                defaultValue: { summary: inputDefaults?.isSuccess?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isDisabled: {
            description: 'Whether the Input is in disabled state.',
            table: {
                defaultValue: { summary: inputDefaults?.isDisabled?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isInvalid: {
            description: 'Whether the Input is an invalid state.',
            table: {
                defaultValue: { summary: inputDefaults?.isInvalid?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isReadOnly: {
            description: 'Whether the Input is in readonly mode.',
            table: {
                defaultValue: { summary: inputDefaults?.isReadOnly?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isPassword: {
            description: 'Whether the Input has a password type.',
            table: {
                defaultValue: { summary: inputDefaults?.isPassword?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isRequired: {
            description: 'Whether the Input is a required field.',
            table: {
                defaultValue: { summary: inputDefaults?.isRequired?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
    },
    args: {
        value: inputDefaults.value,
        label: inputDefaults.label,
        placeholder: inputDefaults.placeholder,
        icon: inputDefaults.icon,
        error: inputDefaults.error,
        warning: inputDefaults.warning,
        isSuccess: inputDefaults.isSuccess,
        isDisabled: inputDefaults.isDisabled,
        isInvalid: inputDefaults.isInvalid,
        isReadOnly: inputDefaults.isReadOnly,
        isPassword: inputDefaults.isPassword,
        isRequired: inputDefaults.isRequired,
    },
} satisfies Meta<typeof Input>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Input {...props} />,
};

export const status = () => (
    <GridList width="400px" isVertical>
        <Input label="Success Status" isSuccess />
        <Input label="Error Status" isInvalid error="Error Message" />
        <Input label="Warning Status" warning="Warning Message" />
    </GridList>
);

export const states = () => (
    <GridList width="400px" isVertical>
        <Input label="Disabled" isDisabled />
        <Input label="Readonly" isReadOnly />
        <Input label="Required" isRequired />
        <Input label="Password" isPassword />
    </GridList>
);
