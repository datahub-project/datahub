import { fireEvent, render, screen, within } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { Table } from '@components/components/Table/Table';
import { Column, SortingState, TableProps } from '@components/components/Table/types';

import themeV2 from '@conf/theme/themeV2';

type Row = { id: string; name: string; children?: Row[] };

const TREE: Row[] = [
    {
        id: 'b',
        name: 'beta',
        children: [
            { id: 'b.2', name: 'beta-2' },
            { id: 'b.1', name: 'beta-1', children: [{ id: 'b.1.x', name: 'beta-1-x' }] },
        ],
    },
    { id: 'a', name: 'alpha' },
];

const COLUMNS: Column<Row>[] = [
    { key: 'name', title: 'Name', dataIndex: 'name', sorter: (x, y) => x.name.localeCompare(y.name) },
];

function renderTable(props: Partial<TableProps<Row>> = {}) {
    return render(
        <ThemeProvider theme={themeV2}>
            <Table<Row> columns={COLUMNS} data={TREE} rowKey="id" {...props} />
        </ThemeProvider>,
    );
}

const visibleNames = () => screen.getAllByTestId('name').map((cell) => cell.textContent);

describe('Table tree data', () => {
    it('ignores children unless childrenColumnName is set', () => {
        renderTable();
        expect(visibleNames()).toEqual(['beta', 'alpha']);
        expect(screen.queryByTestId('table-row-expand-icon')).toBeNull();
    });

    it('expands and collapses nested rows with the expand icon', () => {
        const onExpand = vi.fn();
        const onRowClick = vi.fn();
        renderTable({ expandable: { childrenColumnName: 'children', onExpand }, onRowClick });

        expect(visibleNames()).toEqual(['beta', 'alpha']);
        fireEvent.click(screen.getByTestId('table-row-expand-icon'));
        expect(visibleNames()).toEqual(['beta', 'beta-2', 'beta-1', 'alpha']);
        expect(onExpand).toHaveBeenLastCalledWith(true, TREE[0]);
        expect(onRowClick).not.toHaveBeenCalled();

        fireEvent.click(screen.getAllByTestId('table-row-expand-icon')[0]);
        expect(visibleNames()).toEqual(['beta', 'alpha']);
        expect(onExpand).toHaveBeenLastCalledWith(false, TREE[0]);
    });

    it('honors defaultExpandedRowKeys and defaultExpandAllRows', () => {
        const { unmount } = renderTable({
            expandable: { childrenColumnName: 'children', defaultExpandedRowKeys: ['b'] },
        });
        expect(visibleNames()).toEqual(['beta', 'beta-2', 'beta-1', 'alpha']);
        unmount();

        renderTable({ expandable: { childrenColumnName: 'children', defaultExpandAllRows: true } });
        expect(visibleNames()).toEqual(['beta', 'beta-2', 'beta-1', 'beta-1-x', 'alpha']);
    });

    it('follows controlled expandedRowKeys', () => {
        const onExpand = vi.fn();
        const { rerender } = renderTable({
            expandable: { childrenColumnName: 'children', expandedRowKeys: [], onExpand },
        });
        fireEvent.click(screen.getByTestId('table-row-expand-icon'));
        expect(onExpand).toHaveBeenCalledWith(true, TREE[0]);
        expect(visibleNames()).toEqual(['beta', 'alpha']);

        rerender(
            <ThemeProvider theme={themeV2}>
                <Table<Row>
                    columns={COLUMNS}
                    data={TREE}
                    rowKey="id"
                    expandable={{ childrenColumnName: 'children', expandedRowKeys: ['b', 'b.1'], onExpand }}
                />
            </ThemeProvider>,
        );
        expect(visibleNames()).toEqual(['beta', 'beta-2', 'beta-1', 'beta-1-x', 'alpha']);
    });

    it('renders a custom expand icon with depth', () => {
        renderTable({
            expandable: {
                childrenColumnName: 'children',
                defaultExpandAllRows: true,
                expandIcon: ({ record, depth, expandable }) => (
                    <span data-testid={`icon-${record.id}`}>{`${depth}-${expandable}`}</span>
                ),
            },
        });
        expect(screen.getByTestId('icon-b').textContent).toBe('0-true');
        expect(screen.getByTestId('icon-b.1.x').textContent).toBe('2-false');
    });
});

describe('Table sorting', () => {
    it('applies the default sort and sorts children within their parent', () => {
        const onSortedDataChange = vi.fn();
        renderTable({
            defaultSortColumn: 'name',
            defaultSortOrder: SortingState.ASCENDING,
            onSortedDataChange,
            expandable: { childrenColumnName: 'children', defaultExpandedRowKeys: ['b'] },
        });
        expect(visibleNames()).toEqual(['alpha', 'beta', 'beta-1', 'beta-2']);
        const [sortedData, sortState] = onSortedDataChange.mock.lastCall ?? [];
        expect(sortedData.map((row: Row) => row.id)).toEqual(['a', 'b']);
        expect(sortedData[1].children.map((row: Row) => row.id)).toEqual(['b.1', 'b.2']);
        expect(sortState).toEqual({ sortColumn: 'name', sortOrder: SortingState.ASCENDING });
    });

    it('keeps expansion keyed by rowKey when the sort changes', () => {
        renderTable({ expandable: { childrenColumnName: 'children', defaultExpandedRowKeys: ['b'] } });
        fireEvent.click(screen.getByTestId('sort-name'));
        fireEvent.click(screen.getByTestId('sort-name'));
        expect(visibleNames()).toEqual(['beta', 'beta-2', 'beta-1', 'alpha']);
    });
});

describe('Table rows, columns and empty state', () => {
    it('renders the empty state when there are no rows', () => {
        renderTable({ data: [], emptyState: <span>Nothing here</span> });
        expect(screen.getByText('Nothing here')).toBeInTheDocument();
    });

    it('applies rowProps attributes and merges className with rowClassName', () => {
        const onClick = vi.fn();
        renderTable({
            rowClassName: () => 'from-row-class',
            rowProps: (record) => ({ id: `row-${record.id}`, 'data-kind': 'field', className: 'extra', onClick }),
        });
        const row = document.getElementById('row-a');
        expect(row).toHaveAttribute('data-kind', 'field');
        expect(row).toHaveClass('from-row-class', 'extra');
        fireEvent.click(within(row as HTMLElement).getByText('alpha'));
        expect(onClick).toHaveBeenCalledTimes(1);
    });

    it('pins fixed columns to the left', () => {
        renderTable({
            scrollX: 'max-content',
            columns: [
                { ...COLUMNS[0], fixed: 'left', width: '200px' },
                { key: 'id', title: 'ID', dataIndex: 'id', fixed: 'left', width: '80px' },
            ],
        });
        const [nameCell] = screen.getAllByTestId('name');
        const [idCell] = screen.getAllByTestId('id');
        expect(getComputedStyle(nameCell).position).toBe('sticky');
        expect(getComputedStyle(nameCell).left).toBe('0px');
        expect(getComputedStyle(idCell).left).toBe('calc(200px)');
    });

    it('truncates ellipsis columns while keeping their text', () => {
        renderTable({ columns: [{ ...COLUMNS[0], ellipsis: true }] });
        const [cell] = screen.getAllByTestId('name');
        expect(cell.textContent).toBe('beta');
        expect(getComputedStyle(cell.firstElementChild as Element).textOverflow).toBe('ellipsis');
    });
});
