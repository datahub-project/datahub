import React from 'react';
import type { Meta, StoryObj } from '@storybook/react';
import { GridList } from '@components/.docs/mdx-components';
import styled from 'styled-components';
import { Select, selectDefaults } from './Select';
import { SelectSizeOptions } from './types';

const SelectContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    & > div {
        flex-direction: column;
    }
`;

// Auto Docs
const meta: Meta = {
    title: 'Forms / Select',
    component: Select,

    // Display Properties
    parameters: {
        layout: 'centered',
        docs: {
            subtitle: 'Select component allows users to select one or multiple options from a dropdown list.',
        },
    },

    // Component-level argTypes
    argTypes: {
        options: {
            description: 'Array of options for the Select component.',
            control: {
                type: 'object',
            },
            table: {
                defaultValue: { summary: JSON.stringify(selectDefaults.options) },
            },
        },
        label: {
            description: 'Label for the Select component.',
            control: {
                type: 'text',
            },
            table: {
                defaultValue: { summary: selectDefaults.label },
            },
        },
        value: {
            description: 'Selected value for the Select component.',
            control: {
                type: 'text',
            },
            table: {
                defaultValue: { summary: selectDefaults.value },
            },
        },
        showSearch: {
            description: 'Whether to show the search input.',
            control: {
                type: 'boolean',
            },
            table: {
                defaultValue: { summary: selectDefaults.showSearch?.toString() },
            },
        },
        isDisabled: {
            description: 'Whether the Select component is disabled.',
            control: {
                type: 'boolean',
            },
            table: {
                defaultValue: { summary: selectDefaults.isDisabled?.toString() },
            },
        },
        isReadOnly: {
            description: 'Whether the Select component is read-only.',
            control: {
                type: 'boolean',
            },
            table: {
                defaultValue: { summary: selectDefaults.isReadOnly?.toString() },
            },
        },
        isRequired: {
            description: 'Whether the Select component is required.',
            control: {
                type: 'boolean',
            },
            table: {
                defaultValue: { summary: selectDefaults.isRequired?.toString() },
            },
        },
        size: {
            description: 'Size of the Select component.',
            control: {
                type: 'select',
                options: ['sm', 'md', 'lg'],
            },
            table: {
                defaultValue: { summary: selectDefaults.size },
            },
        },
        width: {
            description: 'Width of the Select component.',
            control: {
                type: 'number',
            },
            table: {
                defaultValue: { summary: `${selectDefaults.width}` },
            },
        },
    },

    // Define defaults
    args: {
        options: [
            { label: 'Option 1', value: '1' },
            { label: 'Option 2', value: '2' },
            { label: 'Option 3', value: '3' },
        ],
        label: 'Select Label',
        value: undefined,
        showSearch: selectDefaults.showSearch,
        isDisabled: selectDefaults.isDisabled,
        isReadOnly: selectDefaults.isReadOnly,
        isRequired: selectDefaults.isRequired,
        onCancel: () => console.log('Cancel clicked'),
        onUpdate: (selectedValues: string[]) => console.log('Update clicked', selectedValues),
        size: 'md', // Default size
        width: 255,
    },
} satisfies Meta<typeof Select>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook & is used as the code sandbox
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],

    render: (props) => (
        <Select
            options={props.options}
            label={props.label}
            value={props.value}
            onCancel={props.onCancel}
            onUpdate={props.onUpdate}
            showSearch={props.showSearch}
            isDisabled={props.isDisabled}
            isReadOnly={props.isReadOnly}
            isRequired={props.isRequired}
            size={props.size}
            width={props.width}
        />
    ),
};

const sizes: SelectSizeOptions[] = ['sm', 'md', 'lg'];

export const states = () => (
    <GridList>
        <>
            <Select options={[{ label: 'Default', value: 'default' }]} label="Default" value="default" />
            <Select
                options={[{ label: 'Disabled State', value: 'disabled' }]}
                label="Disabled State"
                isDisabled
                value="disabled"
            />
            <Select
                options={[{ label: 'Read Only State', value: 'readonly' }]}
                label="Read Only State"
                isReadOnly
                value="readonly"
            />
        </>
    </GridList>
);

export const withSearch = () => (
    <div
        style={{
            display: 'flex',
            height: '250px',
            alignItems: 'baseline',
            justifyContent: 'center',
            margin: '20px',
        }}
    >
        <Select
            options={[
                { label: 'Option 1', value: '1' },
                { label: 'Option 2', value: '2' },
                { label: 'Option 3', value: '3' },
            ]}
            label="Select with Search"
            showSearch
            value="2"
        />
    </div>
);

export const fontSizesAndWidths = () => (
    <SelectContainer>
        <GridList>
            {sizes.map((size, index) => (
                <Select
                    key={`select-${size}`}
                    options={[
                        { label: 'Option 1', value: '1' },
                        { label: 'Option 2', value: '2' },
                        { label: 'Option 3', value: '3' },
                    ]}
                    label={`Select - Font Size: ${size}, Width: ${255 + 50 * index}px`}
                    value="3"
                    onCancel={() => alert('Cancel clicked')}
                    onUpdate={(selectedValues) => alert(`Update clicked with values: ${selectedValues}`)}
                    size={size}
                    width={255 + 50 * index}
                />
            ))}
        </GridList>
    </SelectContainer>
);

export const footerActions = () => (
    <div
        style={{
            display: 'flex',
            height: '250px',
            alignItems: 'baseline',
            justifyContent: 'center',
            margin: '20px',
        }}
    >
        <GridList>
            <Select
                options={[
                    { label: 'Option 1', value: '1' },
                    { label: 'Option 2', value: '2' },
                    { label: 'Option 3', value: '3' },
                ]}
                label="Select with Footer Actions"
                value="3"
                onCancel={() => alert('Cancel clicked')}
                onUpdate={(selectedValues) => alert(`Update clicked with values: ${selectedValues}`)}
                size="md"
            />
        </GridList>
    </div>
);
