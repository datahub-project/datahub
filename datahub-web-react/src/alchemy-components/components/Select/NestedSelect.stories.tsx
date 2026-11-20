import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import { fn } from '@storybook/test';
import React from 'react';

import { NestedSelect } from '@components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@components/components/Select/Nested/types';

const OPTIONS: NestedSelectOption[] = [
    { value: 'marketing', label: 'Marketing', isParent: true },
    { value: 'campaigns', label: 'Campaigns', parentValue: 'marketing' },
    { value: 'web', label: 'Web Analytics', parentValue: 'marketing' },
    { value: 'finance', label: 'Finance', isParent: true },
    { value: 'billing', label: 'Billing', parentValue: 'finance', isParent: true },
    { value: 'invoices', label: 'Invoices', parentValue: 'billing' },
    { value: 'payments', label: 'Payments', parentValue: 'billing' },
    { value: 'forecasting', label: 'Forecasting', parentValue: 'finance' },
    { value: 'engineering', label: 'Engineering' },
];

const meta = {
    title: 'Forms / NestedSelect',
    component: NestedSelect,

    parameters: {
        layout: 'centered',
        badges: [BADGE.STABLE],
        docs: {
            subtitle:
                'Select for hierarchical options: children reference their parent via `parentValue` and render as expandable groups.',
        },
    },

    argTypes: {
        options: {
            description:
                'Flat list of options. Set `isParent` on groups and `parentValue` on children to build the tree.',
            control: { type: 'object' },
        },
        label: {
            description: 'Label rendered above the select.',
            control: { type: 'text' },
            table: { defaultValue: { summary: '""' } },
        },
        initialValues: {
            description: 'Initially selected options (full option objects).',
            control: { type: 'object' },
            table: { defaultValue: { summary: '[]' } },
        },
        isMultiSelect: {
            description: 'Allow selecting multiple options.',
            control: { type: 'boolean' },
            table: { defaultValue: { summary: 'true' } },
        },
        areParentsSelectable: {
            description: 'Whether parent options can be selected themselves.',
            control: { type: 'boolean' },
            table: { defaultValue: { summary: 'true' } },
        },
        showSearch: {
            description: 'Show a search input in the dropdown.',
            control: { type: 'boolean' },
            table: { defaultValue: { summary: 'false' } },
        },
        showClear: {
            description: 'Show a clear button when options are selected.',
            control: { type: 'boolean' },
            table: { defaultValue: { summary: 'false' } },
        },
        shouldDisplayConfirmationFooter: {
            description: 'Stage changes until the user confirms with the Update footer button.',
            control: { type: 'boolean' },
            table: { defaultValue: { summary: 'false' } },
        },
        isDisabled: {
            description: 'Disable the select.',
            control: { type: 'boolean' },
            table: { defaultValue: { summary: 'false' } },
        },
        onUpdate: {
            description: 'Called with the selected option objects.',
            control: false,
        },
        loadData: {
            description: 'Called when a parent without loaded children is expanded, to lazy-load them.',
            control: false,
        },
    },

    args: {
        options: OPTIONS,
        label: 'Teams',
        isMultiSelect: true,
        showSearch: true,
        onUpdate: fn(),
    },
} satisfies Meta<typeof NestedSelect>;

export default meta;

type Story = StoryObj<typeof meta>;

export const sandbox: Story = {
    tags: ['dev'],
    // Without this, NestedSelect reads `initialValues` only on mount, so the control would do nothing.
    render: (args) => <NestedSelect {...args} shouldAlwaysSyncParentValues />,
};

export const singleSelect: Story = {
    args: { label: 'Single team', isMultiSelect: false, areParentsSelectable: false },
};

export const withInitialValues: Story = {
    args: {
        label: 'Preselected',
        initialValues: OPTIONS.filter((option) => option.value === 'billing' || option.value === 'web'),
        showClear: true,
    },
};
