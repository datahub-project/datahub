import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { Globe } from '@phosphor-icons/react/dist/csr/Globe';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Star } from '@phosphor-icons/react/dist/csr/Star';
import type { Meta, StoryObj } from '@storybook/react';
import React, { useEffect, useState } from 'react';

import { GridList } from '@components/.docs/mdx-components';
import { Select, selectDefaults } from '@components/components/Select/Select';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';
import { SelectProps, SelectSizeOptions } from '@components/components/Select/types';

const ICON_OPTIONS = { None: undefined, MagnifyingGlass, Star, Globe };

// Auto Docs
const meta = {
    title: 'Forms / Select',
    component: Select,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'This component allows users to select one or multiple input options from a dropdown list.',
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
        values: {
            description: 'Selected values for the Select component.',
            control: {
                type: 'object',
            },
            table: {
                defaultValue: { summary: '[]' },
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
        minWidth: {
            description: 'Minimal width of the Select component.',
            control: {
                type: 'text',
            },
        },
        isMultiSelect: {
            description: 'Whether the Select component allows multiple values to be selected.',
            control: {
                type: 'boolean',
            },
            table: {
                defaultValue: { summary: selectDefaults.isMultiSelect?.toString() },
            },
        },
        placeholder: {
            description: 'Placeholder for the Select component.',
            control: {
                type: 'text',
            },
            table: {
                defaultValue: {
                    summary:
                        typeof selectDefaults.placeholder === 'string'
                            ? selectDefaults.placeholder
                            : 'Select an option',
                },
            },
        },
        disabledValues: {
            description: 'Disabled values for the multi-select component.',
            control: {
                type: 'object',
            },
            table: {
                defaultValue: { summary: selectDefaults.disabledValues?.toString() },
            },
        },
        showSelectAll: {
            description: 'Whether the multi select component shows Select All button.',
            control: {
                type: 'boolean',
            },
            table: {
                defaultValue: { summary: selectDefaults.showSelectAll?.toString() },
            },
        },
        selectAllLabel: {
            description: 'Label for Select All button.',
            control: {
                type: 'text',
            },
            table: {
                defaultValue: { summary: 'Select All' },
            },
        },
        showDescriptions: {
            description: 'Whether to show descriptions with the select options.',
            control: {
                type: 'boolean',
            },
            table: {
                defaultValue: { summary: selectDefaults.showDescriptions?.toString() },
            },
        },
        icon: {
            description: 'The Phosphor icon component to display.',
            options: Object.keys(ICON_OPTIONS),
            mapping: ICON_OPTIONS,
            table: {
                defaultValue: { summary: 'undefined' },
            },
            control: {
                type: 'select',
            },
        },
        optionSwitchable: {
            description: 'Set to `true` to unselect option by clicking on it',
            type: 'boolean',
            table: {
                defaultValue: { summary: 'undefined' },
            },
            control: {
                type: 'boolean',
            },
        },
        selectLabelProps: {
            description: 'Additional props for rendering of select label',
            table: {
                defaultValue: { summary: 'undefined' },
            },
        },
        emptyState: {
            description: 'Custom empty state component to render when no options are available',
            table: {
                defaultValue: { summary: 'undefined' },
            },
        },
        visibilityDeps: {
            description: 'Additional dependencies to trigger computing of visibility state',
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
        values: undefined,
        showSearch: selectDefaults.showSearch,
        isDisabled: selectDefaults.isDisabled,
        isReadOnly: selectDefaults.isReadOnly,
        isRequired: selectDefaults.isRequired,
        onCancel: () => console.log('Cancel clicked'),
        onUpdate: (selectedValues: string[]) => console.log('Update clicked', selectedValues),
        size: 'md', // Default size
        width: 255,
        isMultiSelect: selectDefaults.isMultiSelect,
        placeholder: selectDefaults.placeholder,
        disabledValues: undefined,
        showSelectAll: false,
        selectAllLabel: 'Select All',
        showDescriptions: false,
    },
} satisfies Meta<typeof Select>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

const sizeOptions: SelectSizeOptions[] = ['sm', 'md', 'lg'];

// Select resets its selection to `values` on every change, so keep the selection in local state.
const ControlledSelect = ({ values: initialValues, onUpdate, ...props }: SelectProps) => {
    const [values, setValues] = useState(initialValues ?? []);
    useEffect(() => setValues(initialValues ?? []), [initialValues]);
    return (
        <Select
            {...props}
            values={values}
            onUpdate={(newValues) => {
                onUpdate?.(newValues);
                setValues(newValues);
            }}
        />
    );
};

export const simpleSelectSandbox: Story = {
    tags: ['dev'],

    render: (props) => <SimpleSelect {...props} />,
};

export const simpleSelectStates = () => (
    <GridList isVertical>
        <>
            <SimpleSelect
                options={[{ label: 'Default', value: 'default' }]}
                label="Default"
                initialValues={['default']}
            />
            <SimpleSelect
                options={[{ label: 'Disabled State', value: 'disabled' }]}
                label="Disabled State"
                isDisabled
                initialValues={['disabled']}
            />
            <SimpleSelect
                options={[{ label: 'Read Only State', value: 'readonly' }]}
                label="Read Only State"
                isReadOnly
                initialValues={['readonly']}
            />
        </>
    </GridList>
);

export const simpleSelectWithSearch = () => (
    <SimpleSelect
        options={[
            { label: 'Option 1', value: '1' },
            { label: 'Option 2', value: '2' },
            { label: 'Option 3', value: '3' },
        ]}
        label="Simple Select with Search"
        showSearch
        initialValues={['2']}
    />
);

export const simpleSelectWithMultiSelect = () => (
    <SimpleSelect
        options={[
            { label: 'Option 1', value: '1' },
            { label: 'Option 2', value: '2' },
            { label: 'Option 3', value: '3' },
        ]}
        label="Simple Select with multi-select"
        showSearch
        initialValues={['2', '3']}
        isMultiSelect
    />
);

export const simpleSelectWithDisabledValues = () => (
    <SimpleSelect
        options={[
            { label: 'Option 1', value: '1' },
            { label: 'Option 2', value: '2' },
            { label: 'Option 3', value: '3' },
        ]}
        label="Simple Select with disabled values"
        initialValues={['2']}
        isMultiSelect
        disabledValues={['2']}
    />
);

export const simpleSelectWithSelectAll = () => (
    <SimpleSelect
        options={[
            { label: 'Option 1', value: '1' },
            { label: 'Option 2', value: '2' },
            { label: 'Option 3', value: '3' },
        ]}
        label="Simple Select with Select All"
        isMultiSelect
        showSelectAll
    />
);

export const simpleSelectWithDescriptions = () => (
    <SimpleSelect
        options={[
            { label: 'Option 1', value: '1', description: 'Description of option 1' },
            { label: 'Option 2', value: '2', description: 'Description of option 2 is  longgggggg' },
            { label: 'Option 3', value: '3', description: 'Description of option 3' },
        ]}
        label="Simple Select with descriptions"
        showDescriptions
    />
);

export const simpleSelectSizes = () => (
    <GridList isVertical>
        {sizeOptions.map((size, index) => (
            <SimpleSelect
                key={`simpleselect-${size}`}
                options={[
                    { label: 'Option 1', value: '1' },
                    { label: 'Option 2', value: '2' },
                    { label: 'Option 3', value: '3' },
                ]}
                label={`Simple Select - Font Size: ${size}, Width: ${255 + 50 * index}px`}
                initialValues={['3']}
                size={size}
                width={255 + 50 * index}
            />
        ))}
    </GridList>
);

export const simpleSelectWithIcon = () => (
    <SimpleSelect
        options={[
            { label: 'Option 1', value: '1' },
            { label: 'Option 2', value: '2' },
            { label: 'Option 3', value: '3' },
        ]}
        label="Simple Select with icon"
        icon={MagnifyingGlass}
    />
);

// Basic story is what is displayed 1st in storybook & is used as the code sandbox
// Pass props to this so that it can be customized via the UI props panel
export const BasicSelectSandbox: Story = {
    tags: ['dev'],

    render: (props) => <ControlledSelect {...props} />,
};

export const states = () => (
    <GridList isVertical>
        <>
            <ControlledSelect options={[{ label: 'Default', value: 'default' }]} label="Default" values={['default']} />
            <ControlledSelect
                options={[{ label: 'Disabled State', value: 'disabled' }]}
                label="Disabled State"
                isDisabled
                values={['disabled']}
            />
            <ControlledSelect
                options={[{ label: 'Read Only State', value: 'readonly' }]}
                label="Read Only State"
                isReadOnly
                values={['readonly']}
            />
        </>
    </GridList>
);

export const withSearch = () => (
    <ControlledSelect
        options={[
            { label: 'Option 1', value: '1' },
            { label: 'Option 2', value: '2' },
            { label: 'Option 3', value: '3' },
        ]}
        label="Select with Search"
        showSearch
        values={['2']}
        filterResultsByQuery
    />
);

export const withMultiSelect = () => (
    <ControlledSelect
        options={[
            { label: 'Option 1', value: '1' },
            { label: 'Option 2', value: '2' },
            { label: 'Option 3', value: '3' },
        ]}
        label="Select with multi-select"
        showSearch
        values={['2', '3']}
        isMultiSelect
    />
);

export const sizes = () => (
    <GridList isVertical>
        {sizeOptions.map((size, index) => (
            <ControlledSelect
                key={`select-${size}`}
                options={[
                    { label: 'Option 1', value: '1' },
                    { label: 'Option 2', value: '2' },
                    { label: 'Option 3', value: '3' },
                ]}
                label={`Select - Font Size: ${size}, Width: ${255 + 50 * index}px`}
                values={['3']}
                onCancel={() => alert('Cancel clicked')}
                onUpdate={(selectedValues) => alert(`Update clicked with values: ${selectedValues}`)}
                size={size}
                width={255 + 50 * index}
            />
        ))}
    </GridList>
);

export const footerActions = () => (
    <GridList isVertical>
        <ControlledSelect
            options={[
                { label: 'Option 1', value: '1' },
                { label: 'Option 2', value: '2' },
                { label: 'Option 3', value: '3' },
            ]}
            label="Select with Footer Actions"
            values={['3']}
            onCancel={() => alert('Cancel clicked')}
            onUpdate={(selectedValues) => alert(`Update clicked with values: ${selectedValues}`)}
            size="md"
        />
    </GridList>
);

export const withIcon = () => (
    <ControlledSelect
        options={[
            { label: 'Option 1', value: '1' },
            { label: 'Option 2', value: '2' },
            { label: 'Option 3', value: '3' },
        ]}
        label="Select with icon"
        values={['2']}
        icon={MagnifyingGlass}
    />
);
