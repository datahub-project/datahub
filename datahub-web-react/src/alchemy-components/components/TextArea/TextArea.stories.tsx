import React from 'react';

import type { Meta, StoryObj } from '@storybook/react';
import { BADGE } from '@geometricpanda/storybook-addon-badges';

import { GridList } from '@components/.docs/mdx-components';

import { TextArea, textAreaDefaults } from './TextArea';
import { AVAILABLE_ICONS } from '../Icon';

// Auto Docs
const meta = {
    title: 'Forms / Text Area',
    component: TextArea,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'A component that is used to get user input in a text area field.',
        },
    },

    // Component-level argTypes
    argTypes: {
        label: {
            description: 'Label for the TextArea.',
            table: {
                defaultValue: { summary: textAreaDefaults.label },
            },
            control: {
                type: 'text',
            },
        },
        placeholder: {
            description: 'Placeholder for the Text Area.',
            table: {
                defaultValue: { summary: textAreaDefaults.placeholder },
            },
            control: {
                type: 'text',
            },
        },
        icon: {
            description: 'The icon to display in the Text Area.',
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
            description: 'Enforce error state on the TextArea.',
            table: {
                defaultValue: { summary: textAreaDefaults.error },
            },
            control: {
                type: 'text',
            },
        },
        warning: {
            description: 'Enforce warning state on the TextArea.',
            table: {
                defaultValue: { summary: textAreaDefaults.warning },
            },
            control: {
                type: 'text',
            },
        },
        isSuccess: {
            description: 'Enforce success state on the TextArea.',
            table: {
                defaultValue: { summary: textAreaDefaults?.isSuccess?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isDisabled: {
            description: 'Enforce disabled state on the TextArea.',
            table: {
                defaultValue: { summary: textAreaDefaults?.isDisabled?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isInvalid: {
            description: 'Enforce invalid state on the TextArea.',
            table: {
                defaultValue: { summary: textAreaDefaults?.isInvalid?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isReadOnly: {
            description: 'Enforce read only state on the TextArea.',
            table: {
                defaultValue: { summary: textAreaDefaults?.isReadOnly?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isRequired: {
            description: 'Enforce required state on the TextArea.',
            table: {
                defaultValue: { summary: textAreaDefaults?.isRequired?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
    },

    // Define defaults
    args: {
        label: textAreaDefaults.label,
        placeholder: textAreaDefaults.placeholder,
        icon: textAreaDefaults.icon,
        error: textAreaDefaults.error,
        warning: textAreaDefaults.warning,
        isSuccess: textAreaDefaults.isSuccess,
        isDisabled: textAreaDefaults.isDisabled,
        isInvalid: textAreaDefaults.isInvalid,
        isReadOnly: textAreaDefaults.isReadOnly,
        isRequired: textAreaDefaults.isRequired,
    },
} satisfies Meta<typeof TextArea>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <TextArea {...props} />,
};

export const status = () => (
    <GridList isVertical>
        <TextArea label="Success Status" isSuccess />
        <TextArea label="Error Status" isInvalid error="Error Message" />
        <TextArea label="Warning Status" warning="Warning Message" />
    </GridList>
);

export const states = () => (
    <GridList isVertical>
        <TextArea label="Disabled TextArea" isDisabled />
        <TextArea label="Readonly TextArea" isReadOnly />
        <TextArea label="Required TextArea" isRequired />
    </GridList>
);
