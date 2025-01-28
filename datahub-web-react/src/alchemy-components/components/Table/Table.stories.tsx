import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';
import { Table, tableDefaults } from '.';
import { SortingState } from './types';

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
        isBorderless: {
            description: 'Whether the table is borderless.',
            control: 'boolean',
            table: {
                defaultValue: { summary: tableDefaults.isBorderless?.toString() },
            },
        },
        onRowClick: {
            description: 'Callback function for row click.',
        },
        rowClassName: {
            description: 'Callback to get class name for the rows.',
        },
        onExpand: {
            description: 'Callback for when a row is expanded.',
        },
        expandable: {
            description: 'Set of props when a row is expandable.',
            control: false,
        },
        rowRefs: {
            description: 'List of refs for table rows.',
            control: false,
        },
        headerRef: {
            description: 'Ref for table header.',
            control: false,
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
        isBorderless: tableDefaults.isBorderless,
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

// Scrollable Table with a maximum height
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

// Table with custom column widths
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

// Table with column sorting functionality
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

// Table without a header
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

// Table with clickable rows
export const ClickableRows = () => {
    const [selectedRow, setSelectedRow] = useState<number | undefined>();

    return (
        <Table
            columns={[
                { title: 'Key', key: 'key', dataIndex: 'key' },
                { title: 'Column 1', key: 'column1', dataIndex: 'column1' },
                { title: 'Column 2', key: 'column2', dataIndex: 'column2' },
            ]}
            data={[
                { key: 1, column1: 'Row 1 Col 1', column2: 'Row 1 Col 2' },
                { key: 2, column1: 'Row 2 Col 1', column2: 'Row 2 Col 2' },
            ]}
            onRowClick={(record) => setSelectedRow(record.key)}
            rowClassName={(record) => (record.key === selectedRow ? 'selected-row' : '')}
        />
    );
};

// Define columns for the grouped table. Each column specifies its title, data key, and optional functionalities like sorting or custom rendering.
const groupByColumns = [
    {
        key: 'id',
        title: 'ID',
        width: '10%',
        render: (record) => <div>{record.id}</div>,
    },
    {
        key: 'name',
        title: 'Name',
        width: '30%',
        render: (record) => <div>{record.name}</div>,
        sorter: (a: any, b: any) => a - b,
    },
    {
        key: 'status',
        title: 'Status',
        width: '20%',
        sorter: (a: any, b: any) => a - b,
        render: (record) => <div>{record.status}</div>,
    },
    {
        key: 'createdAt',
        title: 'Created At',
        width: '25%',
        render: (record) => <div>{record.createdAt}</div>,
    },
    {
        title: '',
        dataIndex: '',
        key: 'actions',
        width: '15%',
        render: (record) => {
            return !record.children && <div>actions</div>;
        },
        alignment: 'right' as AlignmentOptions,
    },
];

// Hierarchical data with parent rows and nested child rows for group-by table functionality.
const groupByData = [
    {
        id: '1',
        name: 'Parent Row 1',
        status: 'Active',
        createdAt: '2024-11-20',
        children: [
            {
                id: '1.1',
                name: 'Child Row 1.1',
                status: 'Active',
                createdAt: '2024-11-21',
            },
            {
                id: '1.2',
                name: 'Child Row 1.2',
                status: 'Inactive',
                createdAt: '2024-11-22',
            },
        ],
    },
    {
        id: '2',
        name: 'Parent Row 2',
        status: 'Inactive',
        createdAt: '2024-11-19',
        children: [
            {
                id: '2.1',
                name: 'Child Row 2.1',
                status: 'Active',
                createdAt: '2024-11-21',
            },
            {
                id: '2.2',
                name: 'Child Row 2.2',
                status: 'Inactive',
                createdAt: '2024-11-22',
            },
        ],
    },
];

// Table with group-by functionality and expandable rows
export const WithGroupByFunctionality = () => {
    const [expandedRowKeys, setExpandedRowKeys] = useState(['Parent Row 1']);
    const [sortedOptions, setSortedOptions] = useState<{ sortColumn: string; sortOrder: SortingState }>({
        sortColumn: '',
        sortOrder: SortingState.ORIGINAL,
    });
    const onExapand = (record: any) => {
        const key = record.name;
        setExpandedRowKeys((prev: any) => (prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]));
    };
    const getSortedRecord = (record) => {
        const { sortOrder, sortColumn } = sortedOptions;
        if (sortOrder === SortingState.ORIGINAL) {
            return record.children;
        }

        const sortFunctions = {
            status: {
                [SortingState.ASCENDING]: (a, b) => a.name.localeCompare(b.name),
                [SortingState.DESCENDING]: (a, b) => b.name.localeCompare(a.name),
            },
            name: {
                [SortingState.ASCENDING]: (a, b) => a.name.localeCompare(b.name),
                [SortingState.DESCENDING]: (a, b) => b.name.localeCompare(a.name),
            },
        };
        const sortFunction = sortFunctions[sortColumn]?.[sortOrder];

        const data = sortFunction ? [...record.children].sort(sortFunction) : record.children;

        return data;
    };

    return (
        <Table
            columns={groupByColumns}
            data={groupByData}
            handleSortColumnChange={({ sortColumn, sortOrder }: { sortColumn: string; sortOrder: SortingState }) =>
                setSortedOptions({ sortColumn, sortOrder })
            }
            expandable={{
                expandedRowRender: (record) => {
                    let sortedRecord = record.children;
                    if (sortedOptions.sortColumn && sortedOptions.sortOrder) {
                        sortedRecord = getSortedRecord(record);
                    }

                    return (
                        <Table
                            columns={groupByColumns}
                            data={sortedRecord}
                            showHeader={false}
                            isBorderless
                            isExpandedInnerTable
                        />
                    );
                },
                rowExpandable: () => true,
                expandIconPosition: 'end',
                expandedRowKeys,
            }}
            onExpand={onExapand}
        />
    );
};
