import React from 'react';
import type { Meta, StoryObj } from '@storybook/react';
import { GridList } from '@components/.docs/mdx-components';
import { Radio, radioDefaults, RadioGroup } from './Radio';
import { Heading } from '../Heading';
import { RadioProps } from './types';

const MOCK_RADIOS: RadioProps[] = [
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
    title: 'Forms / Radio',
    component: Radio,
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'A component that is used to get user input in the state of a radio button.',
        },
    },
    argTypes: {
        label: {
            description: 'Label for the Radio.',
            table: {
                defaultValue: { summary: radioDefaults.label },
            },
            control: {
                type: 'text',
            },
        },
        error: {
            description: 'Enforce error state on the Radio.',
            table: {
                defaultValue: { summary: radioDefaults.error },
            },
            control: {
                type: 'text',
            },
        },
        isChecked: {
            description: 'Whether the Radio is checked.',
            table: {
                defaultValue: { summary: radioDefaults?.isChecked?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isDisabled: {
            description: 'Whether the Radio is in disabled state.',
            table: {
                defaultValue: { summary: radioDefaults?.isDisabled?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
        isRequired: {
            description: 'Whether the Radio is a required field.',
            table: {
                defaultValue: { summary: radioDefaults?.isRequired?.toString() },
            },
            control: {
                type: 'boolean',
            },
        },
    },
    args: {
        label: radioDefaults.label,
        error: radioDefaults.error,
        isChecked: radioDefaults.isChecked,
        isDisabled: radioDefaults.isDisabled,
        isRequired: radioDefaults.isRequired,
    },
} satisfies Meta<typeof Radio>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Radio {...props} />,
};

export const states = () => (
    <GridList>
        <Radio label="Default" />
        <Radio label="Checked" isChecked />
        <Radio label="Error" isChecked error="Error" />
        <Radio label="Required" isChecked isRequired />
    </GridList>
);

export const disabledStates = () => (
    <GridList>
        <Radio label="Default" isDisabled />
        <Radio label="Checked" isChecked isDisabled />
    </GridList>
);

export const radioGroups = () => (
    <GridList isVertical>
        <div>
            <Heading>Horizontal Radio Group</Heading>
            <RadioGroup radios={MOCK_RADIOS} />
        </div>
        <div>
            <Heading>Vertical Radio Group</Heading>
            <RadioGroup isVertical radios={MOCK_RADIOS} />
        </div>
    </GridList>
);
