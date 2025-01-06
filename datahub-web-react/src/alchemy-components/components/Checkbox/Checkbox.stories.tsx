import React from 'react';
import type { Meta, StoryObj } from '@storybook/react';
import { GridList } from '@components/.docs/mdx-components';
import { Checkbox, checkboxDefaults, CheckboxGroup } from './Checkbox';
import { CheckboxProps } from './types';
import { Heading } from '../Heading';

const MOCK_CHECKBOXES: CheckboxProps[] = [
    {
        label: 'Label 1',
        error: '',
        isChecked: false,
        isDisabled: false,
        isIntermediate: false,
        isRequired: false,
    },
    {
        label: 'Label 2',
        error: '',
        isChecked: false,
        isDisabled: false,
        isIntermediate: false,
        isRequired: false,
    },
    {
        label: 'Label 3',
        error: '',
        isChecked: false,
        isDisabled: false,
        isIntermediate: false,
        isRequired: false,
    },
];

const meta = {
    title: 'Forms / Checkbox',
    component: Checkbox,
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'A component that is used to get user input in the state of a check box.',
        },
    },
    argTypes: {
        label: {
            description: 'Label for the Checkbox.',
            table: {
                defaultValue: { summary: checkboxDefaults.label },
            },
            control: {
                type: 'text',
            },
        },
        error: {
            description: 'Enforce error state on the Checkbox.',
            table: {
                defaultValue: { summary: checkboxDefaults.error },
            },
            control: {
                type: 'text',
            },
        },
        isChecked: {
            description: 'Whether the Checkbox is checked.',
            table: {
                defaultValue: { summary: checkboxDefaults?.isChecked?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isDisabled: {
            description: 'Whether the Checkbox is in disabled state.',
            table: {
                defaultValue: { summary: checkboxDefaults?.isDisabled?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isIntermediate: {
            description: 'Whether the Checkbox is in intermediate state.',
            table: {
                defaultValue: { summary: checkboxDefaults?.isIntermediate?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isRequired: {
            description: 'Whether the Checkbox is a required field.',
            table: {
                defaultValue: { summary: checkboxDefaults?.isRequired?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
    },
    args: {
        label: checkboxDefaults.label,
        error: checkboxDefaults.error,
        isChecked: checkboxDefaults.isChecked,
        isDisabled: checkboxDefaults.isDisabled,
        isIntermediate: checkboxDefaults.isIntermediate,
        isRequired: checkboxDefaults.isRequired,
    },
} satisfies Meta<typeof Checkbox>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Checkbox {...props} />,
};

export const states = () => (
    <GridList>
        <Checkbox label="Default" />
        <Checkbox label="Checked" isChecked />
        <Checkbox label="Error" isChecked error="Error" />
        <Checkbox label="Required" isChecked isRequired />
    </GridList>
);

export const intermediate = () => {
    return (
        <GridList>
            <Checkbox label="Primary" isChecked isIntermediate />
            <Checkbox label="Error" isChecked isIntermediate error="Error" />
        </GridList>
    );
};

export const disabledStates = () => (
    <GridList>
        <Checkbox label="Default" isDisabled />
        <Checkbox label="Checked" isChecked isDisabled />
        <Checkbox label="Intermediate" isChecked isDisabled isIntermediate />
    </GridList>
);

export const checkboxGroups = () => (
    <GridList isVertical>
        <div>
            <Heading>Horizontal Checkbox Group</Heading>
            <CheckboxGroup checkboxes={MOCK_CHECKBOXES} />
        </div>
        <div>
            <Heading>Vertical Checkbox Group</Heading>
            <CheckboxGroup isVertical checkboxes={MOCK_CHECKBOXES} />
        </div>
    </GridList>
);
