import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { CheckCircle } from '@phosphor-icons/react/dist/csr/CheckCircle';
import { Circle } from '@phosphor-icons/react/dist/csr/Circle';
import { CircleDashed } from '@phosphor-icons/react/dist/csr/CircleDashed';
import { CircleHalf } from '@phosphor-icons/react/dist/csr/CircleHalf';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';

import { Button } from '@components/components/Button';
import { Icon } from '@components/components/Icon';
import { Column, SortingState, TableGroupByProps, TableSortState } from '@components/components/Table/types';
import { Text } from '@components/components/Text';

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
        rowDetail: {
            description:
                'Opens a full-width detail panel under each row in `expandedRowKeys` (keys from `rowKey`). The caller owns the toggle.',
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
        groupBy: {
            description:
                'Renders rows in collapsible groups inside the same table. Each group gets a header row with a caret, the content from `renderGroupHeader`, a row count, optional actions, and a group checkbox when `rowSelection` is set.',
            control: false,
        },
        treeData: {
            description:
                'Renders nested rows as an expandable tree: `getChildren`, controlled (`expandedRowKeys` + `onExpandedRowsChange`) or uncontrolled (`defaultExpandedRowKeys`) expansion, and `indentSize`. Children sort within their parent. Needs `rowKey`.',
            control: false,
        },
        sortState: {
            description:
                'Controlled sort (`{ sortColumn, sortOrder }`). Pass with `onSortChange`; without it the table keeps its own sort state.',
            control: false,
        },
        virtualization: {
            description:
                'Renders only the rows in view (`{ rowHeight, overscan }`). Every row must be `rowHeight` tall. Needs `isScrollable` and a bounded height.',
            control: false,
        },
        scrollToRowKey: {
            description:
                'Scrolls this row into view whenever the key changes, including rows outside the virtualized window.',
            control: 'text',
        },
        footer: {
            description: 'Optional footer for the table.',
            table: {
                type: {
                    summary: 'React.ReactNode',
                },
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

interface Run {
    id: string;
    question: string;
    result: string;
    expected: string;
    generated: string;
}

const runs: Run[] = [
    {
        id: 'run-1',
        question: 'How many orders shipped last week?',
        result: 'Pass',
        expected: '1,204 orders',
        generated: '1,204 orders shipped between Nov 2 and Nov 8.',
    },
    {
        id: 'run-2',
        question: 'Which region had the highest revenue?',
        result: 'Fail',
        expected: 'EMEA',
        generated: 'North America had the highest revenue.',
    },
    {
        id: 'run-3',
        question: 'What is the average order value?',
        result: 'Pass',
        expected: '$86.40',
        generated: 'The average order value is $86.40.',
    },
];

// A full-width detail panel under a row. The caller owns the toggle (here a caret column) and passes the
// open rows' keys; use `groupBy` for groups and `treeData` for nested rows instead.
export const WithRowDetail = () => {
    const [expandedKeys, setExpandedKeys] = useState<string[]>(['run-2']);
    const toggle = (id: string) =>
        setExpandedKeys((keys) => (keys.includes(id) ? keys.filter((key) => key !== id) : [...keys, id]));

    return (
        <Table
            columns={[
                {
                    title: '',
                    key: 'toggle',
                    width: '48px',
                    render: (run: Run) => (
                        <Button
                            variant="text"
                            color="gray"
                            size="xs"
                            isCircle
                            icon={{ icon: expandedKeys.includes(run.id) ? CaretDown : CaretRight, color: 'icon' }}
                            aria-expanded={expandedKeys.includes(run.id)}
                            aria-label={expandedKeys.includes(run.id) ? 'Hide answers' : 'Show answers'}
                            onClick={(e) => {
                                e.stopPropagation();
                                toggle(run.id);
                            }}
                        />
                    ),
                },
                { title: 'Question', key: 'question', dataIndex: 'question' },
                { title: 'Result', key: 'result', dataIndex: 'result', width: '120px' },
            ]}
            data={runs}
            rowKey="id"
            rowDetail={{
                expandedRowKeys: expandedKeys,
                render: (run: Run) => (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 4, padding: 16 }}>
                        <Text size="sm" color="textSecondary">
                            Expected: {run.expected}
                        </Text>
                        <Text size="sm">Generated: {run.generated}</Text>
                    </div>
                ),
            }}
        />
    );
};

export const WithRowSelection = () => {
    const [selectedKeys, setSelectedKeys] = useState<string[]>([]);

    return (
        <Table
            columns={[
                { title: 'ID', key: 'id', dataIndex: 'id' },
                { title: 'Column 1', key: 'column1', dataIndex: 'column1' },
                { title: 'Column 2', key: 'column2', dataIndex: 'column2' },
            ]}
            data={[
                { id: '1', column1: 'Row 1 Col 1', column2: 'Row 1 Col 2', column3: 2 },
                { id: '2', column1: 'Row 2 Col 1', column2: 'Row 2 Col 2', column3: 3 },
                { id: '3', column1: 'Row 3 Col 1', column2: 'Row 3 Col 2', column3: 1 },
            ]}
            rowKey="id"
            rowSelection={{
                selectedRowKeys: selectedKeys,
                onChange: (keys) => setSelectedKeys(keys),
            }}
        />
    );
};

interface Issue {
    id: string;
    title: string;
    status: 'Backlog' | 'Todo' | 'In Progress' | 'Done';
    assignee: string;
    estimate: number;
}

const ISSUE_STATUSES: { key: Issue['status']; icon: typeof Circle }[] = [
    { key: 'Backlog', icon: CircleDashed },
    { key: 'Todo', icon: Circle },
    { key: 'In Progress', icon: CircleHalf },
    { key: 'Done', icon: CheckCircle },
];

const issues: Issue[] = [
    { id: 'ROW-101', title: 'Add grouping to the table', status: 'In Progress', assignee: 'Sofia', estimate: 3 },
    { id: 'ROW-102', title: 'Sticky group headers', status: 'Todo', assignee: 'Kenji', estimate: 2 },
    { id: 'ROW-103', title: 'Group-level selection', status: 'Todo', assignee: 'Lena', estimate: 1 },
    { id: 'ROW-104', title: 'Keyboard support for collapse', status: 'Backlog', assignee: 'Tom', estimate: 1 },
    { id: 'ROW-105', title: 'Storybook docs', status: 'Done', assignee: 'Marcus', estimate: 1 },
    { id: 'ROW-106', title: 'Migrate the settings page', status: 'Backlog', assignee: 'Sofia', estimate: 5 },
    { id: 'ROW-107', title: 'Empty group state', status: 'Backlog', assignee: 'Kenji', estimate: 2 },
    { id: 'ROW-108', title: 'Tests for collapse', status: 'Done', assignee: 'Lena', estimate: 1 },
    { id: 'ROW-109', title: 'Group ordering', status: 'Todo', assignee: 'Tom', estimate: 2 },
];

const issueColumns: Column<Issue>[] = [
    { title: 'ID', key: 'id', dataIndex: 'id', width: '120px' },
    { title: 'Title', key: 'title', dataIndex: 'title' },
    { title: 'Assignee', key: 'assignee', dataIndex: 'assignee', width: '160px' },
    {
        title: 'Estimate',
        key: 'estimate',
        dataIndex: 'estimate',
        width: '120px',
        sorter: (a: Issue, b: Issue) => a.estimate - b.estimate,
    },
];

const statusOrder = (key: string) => ISSUE_STATUSES.findIndex((status) => status.key === key);

const issueGroupBy: TableGroupByProps<Issue> = {
    getGroupKey: (issue) => issue.status,
    groupOrder: (a, b) => statusOrder(a.key) - statusOrder(b.key),
    renderGroupHeader: ({ key }) => {
        const status = ISSUE_STATUSES.find((s) => s.key === key);
        return (
            <>
                {status && <Icon icon={status.icon} size="md" color="icon" />}
                {key}
            </>
        );
    },
    renderGroupActions: () => (
        <Button
            variant="text"
            color="gray"
            size="sm"
            isCircle
            icon={{ icon: Plus, color: 'icon' }}
            aria-label="Add issue"
        />
    ),
};

// Rows grouped Linear-style: group header rows live inside the one table, under a single column header.
export const Grouped = () => <Table columns={issueColumns} data={issues} rowKey="id" groupBy={issueGroupBy} />;

// The group checkbox selects that group's rows; the header checkbox still selects every row.
export const GroupedWithSelection = () => {
    const [selectedKeys, setSelectedKeys] = useState<string[]>([]);

    return (
        <Table
            columns={issueColumns}
            data={issues}
            rowKey="id"
            groupBy={issueGroupBy}
            rowSelection={{
                selectedRowKeys: selectedKeys,
                onChange: (keys) => setSelectedKeys(keys),
                getCheckboxProps: (issue: Issue) => ({ disabled: issue.status === 'Done' }),
            }}
        />
    );
};

// Collapsed state can be uncontrolled (`defaultCollapsedKeys`) or controlled (`collapsedKeys` + `onCollapsedChange`).
export const GroupedWithCollapsedGroups = () => {
    const [collapsedKeys, setCollapsedKeys] = useState<string[]>(['Backlog', 'Done']);

    return (
        <Table
            columns={issueColumns}
            data={issues}
            rowKey="id"
            groupBy={{ ...issueGroupBy, collapsedKeys, onCollapsedChange: setCollapsedKeys }}
        />
    );
};

// Group header rows stay pinned under the column header while their rows scroll.
export const GroupedWithStickyHeaders = () => (
    <Table
        columns={issueColumns}
        data={[...issues, ...issues.map((issue) => ({ ...issue, id: `${issue.id}-copy` }))]}
        rowKey="id"
        groupBy={{ ...issueGroupBy, stickyGroupHeaders: true }}
        isScrollable
        maxHeight="320px"
    />
);

interface SchemaField {
    fieldPath: string;
    name: string;
    type: string;
    description: string;
    tags: string;
    terms: string;
    children?: SchemaField[];
}

const FIELD_TYPES = ['string', 'number', 'boolean', 'date', 'struct'];

const makeFields = (count: number, prefix = '', depth = 0): SchemaField[] =>
    Array.from({ length: count }, (_, i) => {
        const name = `${depth === 0 ? 'column' : 'field'}_${i + 1}`;
        const fieldPath = prefix ? `${prefix}.${name}` : name;
        const isStruct = depth < 2 && i % 4 === 0;
        return {
            fieldPath,
            name,
            type: isStruct ? 'struct' : FIELD_TYPES[i % 4],
            description:
                i % 3 === 0 ? '' : `Description for ${fieldPath}, long enough to need truncating in a narrow cell`,
            tags: i % 5 === 0 ? 'PII' : '',
            terms: i % 7 === 0 ? 'Revenue' : '',
            children: isStruct ? makeFields(5, fieldPath, depth + 1) : undefined,
        };
    });

const schemaColumns: Column<SchemaField>[] = [
    {
        title: 'Name',
        key: 'name',
        dataIndex: 'name',
        fixed: 'left',
        width: '260px',
        minWidth: '260px',
        sorter: (a: SchemaField, b: SchemaField) => a.name.localeCompare(b.name, undefined, { numeric: true }),
    },
    {
        title: 'Type',
        key: 'type',
        dataIndex: 'type',
        minWidth: '120px',
        sorter: (a: SchemaField, b: SchemaField) => a.type.localeCompare(b.type),
    },
    { title: 'Description', key: 'description', dataIndex: 'description', minWidth: '420px', maxWidth: '420px' },
    { title: 'Tags', key: 'tags', dataIndex: 'tags', minWidth: '160px' },
    { title: 'Glossary terms', key: 'terms', dataIndex: 'terms', minWidth: '200px' },
    { title: 'Owner', key: 'owner', render: () => 'Data Platform', minWidth: '200px' },
    { title: 'Last modified', key: 'modified', render: () => '3 days ago', minWidth: '180px' },
];

// Nested rows: struct fields expand into their children, indented one level per depth.
export const NestedRows = () => (
    <Table
        columns={schemaColumns.slice(0, 3)}
        data={makeFields(8)}
        rowKey="fieldPath"
        treeData={{ getChildren: (field) => field.children, defaultExpandedRowKeys: ['column_1'] }}
    />
);

// The pinned Name column (and selection column) stays put while the rest scrolls horizontally.
export const PinnedColumns = () => {
    const [selectedKeys, setSelectedKeys] = useState<string[]>([]);
    return (
        <div style={{ maxWidth: 720 }}>
            <Table
                columns={schemaColumns}
                data={makeFields(12)}
                rowKey="fieldPath"
                isScrollable
                maxHeight="420px"
                rowSelection={{ selectedRowKeys: selectedKeys, onChange: (keys) => setSelectedKeys(keys) }}
            />
        </div>
    );
};

// 5,000 rows, only the visible window is in the DOM.
export const Virtualized = () => (
    <Table
        columns={schemaColumns.slice(0, 4)}
        data={makeFields(5000).map((field) => ({ ...field, children: undefined }))}
        rowKey="fieldPath"
        isScrollable
        maxHeight="480px"
        virtualization={{ rowHeight: 55 }}
    />
);

// Everything the schema table needs together: nested rows, pinned name column, virtualization,
// controlled sort, a selected row that the arrow keys move and keep in view.
export const SchemaTable = () => {
    const [data] = useState(() => makeFields(2000));
    const [expandedKeys, setExpandedKeys] = useState<string[]>(['column_1']);
    const [sortState, setSortState] = useState<TableSortState>({ sortColumn: null, sortOrder: SortingState.ORIGINAL });
    const [selectedKey, setSelectedKey] = useState<string | null>('column_1.field_2');

    // The caller owns the visible order (the schema drawer needs it for previous / next).
    const visibleKeys: string[] = [];
    const collect = (fields: SchemaField[]) => {
        const sorter = schemaColumns.find((column) => column.key === sortState.sortColumn)?.sorter;
        let ordered = fields;
        if (typeof sorter === 'function' && sortState.sortOrder !== SortingState.ORIGINAL) {
            ordered = fields
                .slice()
                .sort((a, b) => (sortState.sortOrder === SortingState.ASCENDING ? sorter(a, b) : sorter(b, a)));
        }
        ordered.forEach((field) => {
            visibleKeys.push(field.fieldPath);
            if (field.children && expandedKeys.includes(field.fieldPath)) collect(field.children);
        });
    };
    collect(data);

    const move = (step: number) => {
        const index = selectedKey ? visibleKeys.indexOf(selectedKey) : -1;
        const next = visibleKeys[Math.min(visibleKeys.length - 1, Math.max(0, index + step))];
        if (next) setSelectedKey(next);
    };

    return (
        <div
            role="presentation"
            tabIndex={-1}
            onKeyDown={(e) => {
                if (e.key === 'ArrowDown') move(1);
                if (e.key === 'ArrowUp') move(-1);
            }}
            style={{ display: 'flex', flexDirection: 'column', gap: 8, maxWidth: 960, outline: 'none' }}
        >
            <Text size="sm" color="textSecondary">
                Click a row, then use the up and down arrow keys. Selected: {selectedKey ?? 'none'}
            </Text>
            <Table
                columns={schemaColumns}
                data={data}
                rowKey="fieldPath"
                isScrollable
                maxHeight="520px"
                virtualization={{ rowHeight: 55 }}
                treeData={{
                    getChildren: (field) => field.children,
                    expandedRowKeys: expandedKeys,
                    onExpandedRowsChange: setExpandedKeys,
                }}
                sortState={sortState}
                onSortChange={setSortState}
                focusedRowKey={selectedKey}
                scrollToRowKey={selectedKey}
                onRowClick={(field) => setSelectedKey(field.fieldPath === selectedKey ? null : field.fieldPath)}
            />
        </div>
    );
};
