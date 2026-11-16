import { fireEvent, render, screen, within } from '@testing-library/react';
import React, { useState } from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { Table } from '@components/components/Table/Table';
import { Column, TableGroupByProps } from '@components/components/Table/types';

import themeV2 from '@conf/theme/themeV2';

interface Task {
    id: string;
    name: string;
    status: string;
}

const TASKS: Task[] = [
    { id: '1', name: 'Write docs', status: 'Todo' },
    { id: '2', name: 'Fix bug', status: 'Backlog' },
    { id: '3', name: 'Ship it', status: 'Todo' },
    { id: '4', name: 'Plan', status: 'Backlog' },
    { id: '5', name: 'Review', status: 'Done' },
];

const columns: Column<Task>[] = [{ title: 'Name', key: 'name', dataIndex: 'name' }];

const baseGroupBy: TableGroupByProps<Task> = {
    getGroupKey: (task) => task.status,
    renderGroupHeader: ({ key }) => <span>{key}</span>,
    groupHeaderDataTestId: ({ key }) => `group-${key}`,
};

const renderTable = (props: Partial<React.ComponentProps<typeof Table<Task>>> = {}) =>
    render(
        <ThemeProvider theme={themeV2}>
            <Table<Task>
                columns={columns}
                data={TASKS}
                rowKey="id"
                rowDataTestId={(task) => `row-${task.id}`}
                groupBy={baseGroupBy}
                {...props}
            />
        </ThemeProvider>,
    );

const renderedOrder = () =>
    Array.from(document.querySelectorAll('tbody tr')).map(
        (row) => row.getAttribute('data-testid')?.replace(/^(group|row)-/, '') ?? '',
    );

const SelectableTable = ({ onChange }: { onChange: (keys: string[]) => void }) => {
    const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
    return (
        <ThemeProvider theme={themeV2}>
            <Table<Task>
                columns={columns}
                data={TASKS}
                rowKey="id"
                groupBy={baseGroupBy}
                rowSelection={{
                    selectedRowKeys,
                    getCheckboxProps: (task: Task) => ({ disabled: task.id === '4' }),
                    onChange: (keys) => {
                        setSelectedRowKeys(keys);
                        onChange(keys);
                    },
                }}
            />
        </ThemeProvider>
    );
};

describe('Table without grouping', () => {
    it('renders rows in data order with no group rows, and row selection still works', () => {
        const onChange = vi.fn();
        render(
            <ThemeProvider theme={themeV2}>
                <Table<Task>
                    columns={columns}
                    data={TASKS}
                    rowKey="id"
                    rowDataTestId={(task) => `row-${task.id}`}
                    rowSelection={{ selectedRowKeys: ['1'], onChange }}
                />
            </ThemeProvider>,
        );

        expect(renderedOrder()).toEqual(['1', '2', '3', '4', '5']);
        // The row title styling goes to the first data column, not the checkbox column.
        const [checkboxCell, nameCell] = Array.from(screen.getByTestId('row-3').querySelectorAll('td'));
        expect(nameCell).toHaveStyle({ fontWeight: '700' });
        expect(checkboxCell).not.toHaveStyle({ fontWeight: '700' });
        fireEvent.click(within(screen.getByTestId('row-3')).getByRole('checkbox'));
        expect(onChange).toHaveBeenLastCalledWith(['1', '3'], [TASKS[0], TASKS[2]]);
    });
});

describe('Table grouping', () => {
    it('renders group header rows inside one table, in first-appearance order, with counts', () => {
        renderTable();

        expect(document.querySelectorAll('table')).toHaveLength(1);
        expect(document.querySelectorAll('thead')).toHaveLength(1);
        expect(renderedOrder()).toEqual(['Todo', '1', '3', 'Backlog', '2', '4', 'Done', '5']);
        expect(within(screen.getByTestId('group-Todo')).getByText('2')).toBeInTheDocument();
        expect(screen.getByTestId('group-Todo').querySelector('td')).toHaveAttribute('colspan', '1');
    });

    it('orders groups with groupOrder and keeps row order inside each group', () => {
        renderTable({
            groupBy: {
                ...baseGroupBy,
                groupOrder: (a, b) => b.rows.length - a.rows.length || a.key.localeCompare(b.key),
            },
        });

        expect(renderedOrder()).toEqual(['Backlog', '2', '4', 'Todo', '1', '3', 'Done', '5']);
    });

    it('collapses and expands a group from its caret button', () => {
        renderTable({ groupBy: { ...baseGroupBy, defaultCollapsedKeys: ['Done'] } });

        expect(screen.queryByTestId('row-5')).not.toBeInTheDocument();

        const todoToggle = within(screen.getByTestId('group-Todo')).getByRole('button', { name: 'Collapse group' });
        expect(todoToggle).toHaveAttribute('aria-expanded', 'true');
        fireEvent.click(todoToggle);

        expect(screen.queryByTestId('row-1')).not.toBeInTheDocument();
        expect(screen.queryByTestId('row-3')).not.toBeInTheDocument();
        expect(within(screen.getByTestId('group-Todo')).getByRole('button', { name: 'Expand group' })).toHaveAttribute(
            'aria-expanded',
            'false',
        );

        fireEvent.click(within(screen.getByTestId('group-Done')).getByRole('button', { name: 'Expand group' }));
        expect(screen.getByTestId('row-5')).toBeInTheDocument();
    });

    it('reports collapse changes and follows collapsedKeys when controlled', () => {
        const onCollapsedChange = vi.fn();
        renderTable({ groupBy: { ...baseGroupBy, collapsedKeys: ['Backlog'], onCollapsedChange } });

        fireEvent.click(screen.getByTestId('group-Todo'));

        expect(onCollapsedChange).toHaveBeenCalledWith(['Backlog', 'Todo']);
        // Controlled: nothing changes until the caller passes new keys.
        expect(screen.getByTestId('row-1')).toBeInTheDocument();
        expect(screen.queryByTestId('row-2')).not.toBeInTheDocument();
    });

    it('selects and deselects only the selectable rows of a group', () => {
        const onChange = vi.fn();
        render(<SelectableTable onChange={onChange} />);

        const backlogCheckbox = () =>
            within(screen.getByTestId('group-Backlog')).getByRole('checkbox', { name: 'Select all rows in group' });

        fireEvent.click(backlogCheckbox());
        expect(onChange).toHaveBeenLastCalledWith(['2']);
        expect(backlogCheckbox()).toBeChecked();

        fireEvent.click(backlogCheckbox());
        expect(onChange).toHaveBeenLastCalledWith([]);
    });

    it('keeps top-level select-all working across groups, including collapsed ones', () => {
        const onChange = vi.fn();
        render(<SelectableTable onChange={onChange} />);
        fireEvent.click(within(screen.getByTestId('group-Done')).getByRole('button', { name: 'Collapse group' }));

        const selectAll = within(document.querySelector('thead') as HTMLElement).getByRole('checkbox');
        fireEvent.click(selectAll);

        expect([...(onChange.mock.lastCall?.[0] ?? [])].sort()).toEqual(['1', '2', '3', '5']);
        expect(
            within(screen.getByTestId('group-Todo')).getByRole('checkbox', { name: 'Select all rows in group' }),
        ).toBeChecked();
    });
});
