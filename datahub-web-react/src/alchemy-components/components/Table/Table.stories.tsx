import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import { Table, tableDefaults } from '.';

// Auto Docs
const meta = {
    title: 'Lists & Tables / Table',
    component: Table,

    // Display Properties
    parameters: {
        layout: 'padded',
        badges: [BADGE.STABLE, 'readyForDesignReview'],
        docs: {
            subtitle: 'This component allows users to render a table with different columns and their data',
        },
    },

    // Component-level argTypes
    argTypes: {
        columns: {
            description: 'Array of column objects for the table header.',
            control: 'object',
            table: {
                defaultValue: { summary: JSON.stringify(tableDefaults.columns) },
            },
        },
        data: {
            description: 'Array of data rows for the table body.',
            control: 'object',
            table: {
                defaultValue: { summary: JSON.stringify(tableDefaults.data) },
            },
        },
        showHeader: {
            description: 'Whether to show the table header.',
            control: 'boolean',
            table: {
                defaultValue: { summary: tableDefaults.showHeader?.toString() },
            },
        },
        isLoading: {
            description: 'Whether the table is in loading state.',
            control: 'boolean',
            table: {
                defaultValue: { summary: tableDefaults.isLoading?.toString() },
            },
        },
        isScrollable: {
            description: 'Whether the table is scrollable.',
            control: 'boolean',
            table: {
                defaultValue: { summary: tableDefaults.isScrollable?.toString() },
            },
        },
        maxHeight: {
            description: 'Maximum height of the table container.',
            control: 'text',
            table: {
                defaultValue: { summary: tableDefaults.maxHeight },
            },
        },
    },

    // Define defaults
    args: {
        columns: [
            { title: 'Column 1', key: 'column1', dataIndex: 'column1' },
            { title: 'Column 2', key: 'column2', dataIndex: 'column2' },
        ],
        data: [
            { column1: 'Row 1 Col 1', column2: 'Row 1 Col 2' },
            { column1: 'Row 2 Col 1', column2: 'Row 2 Col 2' },
        ],
        showHeader: tableDefaults.showHeader,
        isLoading: tableDefaults.isLoading,
        isScrollable: tableDefaults.isScrollable,
        maxHeight: tableDefaults.maxHeight,
    },
} satisfies Meta<typeof Table<any>>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

// Basic story is what is displayed 1st in storybook & is used as the code sandbox
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => <Table {...props} />,
};

export const withScroll = () => (
    <Table
        columns={[
            { title: 'Column 1', key: 'column1', dataIndex: 'column1' },
            { title: 'Column 2', key: 'column2', dataIndex: 'column2' },
        ]}
        data={[
            { column1: 'Row 1 Col 1', column2: 'Row 1 Col 2' },
            { column1: 'Row 2 Col 1', column2: 'Row 2 Col 2' },
            { column1: 'Row 3 Col 1', column2: 'Row 3 Col 2' },
            { column1: 'Row 4 Col 1', column2: 'Row 4 Col 2' },
            { column1: 'Row 5 Col 1', column2: 'Row 5 Col 2' },
            { column1: 'Row 6 Col 1', column2: 'Row 6 Col 2' },
        ]}
        maxHeight="180px"
        isScrollable
    />
);

export const withCustomColumnWidths = () => (
    <Table
        columns={[
            { title: 'Column 1', key: 'column1', dataIndex: 'column1', width: '50%' },
            { title: 'Column 2', key: 'column2', dataIndex: 'column2', width: '30%' },
            { title: 'Column 3', key: 'column3', dataIndex: 'column3', width: '20%' },
        ]}
        data={[
            { column1: 'Row 1 Col 1', column2: 'Row 1 Col 2', column3: 1 },
            { column1: 'Row 2 Col 1', column2: 'Row 2 Col 2', column3: 2 },
            { column1: 'Row 3 Col 1', column2: 'Row 3 Col 2', column3: 3 },
        ]}
    />
);

export const withColumnSorting = () => (
    <Table
        columns={[
            {
                title: 'Column 1',
                key: 'column1',
                dataIndex: 'column1',
                sorter: (a, b) => a.column1.localeCompare(b.column1),
            },
            { title: 'Column 2', key: 'column2', dataIndex: 'column2' },
            { title: 'Column 3', key: 'column3', dataIndex: 'column3', sorter: (a, b) => a.column3 - b.column3 },
        ]}
        data={[
            { column1: 'Row 2 Col 1', column2: 'Row 2 Col 2', column3: 3 },
            { column1: 'Row 1 Col 1', column2: 'Row 1 Col 2', column3: 2 },
            { column1: 'Row 3 Col 1', column2: 'Row 3 Col 2', column3: 1 },
        ]}
    />
);

export const withoutHeader = () => (
    <Table
        columns={[
            { title: 'Column 1', key: 'column1', dataIndex: 'column1' },
            { title: 'Column 2', key: 'column2', dataIndex: 'column2' },
        ]}
        data={[
            { column1: 'Row 1 Col 1', column2: 'Row 1 Col 2' },
            { column1: 'Row 2 Col 1', column2: 'Row 2 Col 2' },
        ]}
        showHeader={false}
    />
);
