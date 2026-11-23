import { act, fireEvent, render, screen, within } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { Table } from '@components/components/Table/Table';
import { Column, SortingState, TableProps } from '@components/components/Table/types';

import themeV2 from '@conf/theme/themeV2';

interface Field {
    path: string;
    type: string;
    children?: Field[];
}

const FIELDS: Field[] = [
    {
        path: 'customer',
        type: 'struct',
        children: [
            { path: 'customer.name', type: 'string' },
            {
                path: 'customer.address',
                type: 'struct',
                children: [{ path: 'customer.address.city', type: 'string' }],
            },
        ],
    },
    { path: 'amount', type: 'number' },
];

const columns: Column<Field>[] = [
    {
        title: 'Name',
        key: 'path',
        dataIndex: 'path',
        fixed: 'left',
        sorter: (a: Field, b: Field) => a.path.localeCompare(b.path),
    },
    { title: 'Type', key: 'type', dataIndex: 'type' },
];

const renderTable = (props: Partial<TableProps<Field>> = {}) =>
    render(
        <ThemeProvider theme={themeV2}>
            <Table<Field>
                columns={columns}
                data={FIELDS}
                rowKey="path"
                rowDataTestId={(field) => `row-${field.path}`}
                treeData={{ getChildren: (field) => field.children }}
                {...props}
            />
        </ThemeProvider>,
    );

const renderedRows = () =>
    Array.from(document.querySelectorAll('tbody tr[data-row-key]')).map((row) => row.getAttribute('data-row-key'));

describe('Table nested rows', () => {
    it('shows children only after their parent is expanded, indented under it', () => {
        const onRowClick = vi.fn();
        renderTable({ onRowClick });

        expect(renderedRows()).toEqual(['customer', 'amount']);
        const expand = within(screen.getByTestId('row-customer')).getByRole('button', { name: 'Expand row' });
        expect(expand).toHaveAttribute('aria-expanded', 'false');
        expect(within(screen.getByTestId('row-amount')).queryByRole('button')).not.toBeInTheDocument();

        fireEvent.click(expand);

        expect(renderedRows()).toEqual(['customer', 'customer.name', 'customer.address', 'amount']);
        expect(onRowClick).not.toHaveBeenCalled();

        fireEvent.click(within(screen.getByTestId('row-customer')).getByRole('button', { name: 'Collapse row' }));
        expect(renderedRows()).toEqual(['customer', 'amount']);
    });

    it('keeps rows below an expanded parent mounted', () => {
        renderTable();
        const amountRow = screen.getByTestId('row-amount');

        fireEvent.click(within(screen.getByTestId('row-customer')).getByRole('button', { name: 'Expand row' }));

        expect(screen.getByTestId('row-amount')).toBe(amountRow);
    });

    it('includes nested rows in selection, expanded or not', () => {
        const onChange = vi.fn();
        renderTable({ rowSelection: { selectedRowKeys: [], onChange } });

        fireEvent.click(within(document.querySelector('thead') as HTMLElement).getByRole('checkbox'));
        expect([...onChange.mock.lastCall?.[0]].sort()).toEqual([
            'amount',
            'customer',
            'customer.address',
            'customer.address.city',
            'customer.name',
        ]);

        fireEvent.click(within(screen.getByTestId('row-customer')).getByRole('button', { name: 'Expand row' }));
        fireEvent.click(within(screen.getByTestId('row-customer.name')).getByRole('checkbox'));
        expect(onChange).toHaveBeenLastCalledWith(['customer.name'], [FIELDS[0].children?.[0]]);
    });

    it('sorts children within their parent', () => {
        renderTable({ treeData: { getChildren: (field) => field.children, defaultExpandedRowKeys: ['customer'] } });

        fireEvent.click(screen.getByTestId('sort-path')); // ascending
        fireEvent.click(screen.getByTestId('sort-path')); // descending

        expect(renderedRows()).toEqual(['customer', 'customer.name', 'customer.address', 'amount']);
    });

    it('follows expandedRowKeys when controlled', () => {
        const onExpandedRowsChange = vi.fn();
        renderTable({
            treeData: {
                getChildren: (field) => field.children,
                expandedRowKeys: ['customer', 'customer.address'],
                onExpandedRowsChange,
            },
        });

        expect(renderedRows()).toContain('customer.address.city');
        fireEvent.click(within(screen.getByTestId('row-customer')).getByRole('button', { name: 'Collapse row' }));

        expect(onExpandedRowsChange).toHaveBeenCalledWith(['customer.address']);
        expect(renderedRows()).toContain('customer.address.city');
    });
});

describe('Table row detail', () => {
    it('renders a full-width panel under the rows whose keys are open', () => {
        renderTable({
            treeData: undefined,
            rowDetail: { expandedRowKeys: ['amount'], render: (field) => `Details for ${field.path}` },
        });

        const panels = screen.getAllByTestId('table-row-detail');
        expect(panels).toHaveLength(1);
        expect(panels[0]).toHaveTextContent('Details for amount');
        expect(panels[0].previousElementSibling).toBe(screen.getByTestId('row-amount'));
        expect(within(panels[0]).getByRole('cell')).toHaveAttribute('colspan', '2');
    });
});

describe('Table controlled sort', () => {
    it('reports the next sort and renders rows in the order given by sortState', () => {
        const onSortChange = vi.fn();
        renderTable({
            treeData: undefined,
            sortState: { sortColumn: 'path', sortOrder: SortingState.DESCENDING },
            onSortChange,
        });

        expect(renderedRows()).toEqual(['customer', 'amount']);
        fireEvent.click(screen.getByTestId('sort-path'));

        expect(onSortChange).toHaveBeenCalledWith({ sortColumn: 'path', sortOrder: SortingState.ORIGINAL });
        // Controlled: the order holds until the caller passes a new sortState.
        expect(renderedRows()).toEqual(['customer', 'amount']);
    });
});

describe('Table pinned columns', () => {
    it('pins leading fixed columns and leaves the rest scrolling', () => {
        renderTable();

        const [nameHeader, typeHeader] = Array.from(document.querySelectorAll('th'));
        expect(nameHeader).toHaveStyle({ position: 'sticky' });
        expect(typeHeader).not.toHaveStyle({ position: 'sticky' });
        expect(within(screen.getByTestId('row-amount')).getByTestId('path')).toHaveStyle({ position: 'sticky' });
        expect(within(screen.getByTestId('row-amount')).getByTestId('type')).not.toHaveStyle({ position: 'sticky' });
    });
});

describe('Table memoized cells', () => {
    it('does not re-render cells when the parent re-renders with the same columns and rows', () => {
        const renderType = vi.fn((field: Field) => field.type);
        const spiedColumns: Column<Field>[] = [columns[0], { ...columns[1], render: renderType }];
        const table = (focusedRowKey: string | null) => (
            <ThemeProvider theme={themeV2}>
                <Table<Field>
                    columns={spiedColumns}
                    data={FIELDS}
                    rowKey="path"
                    memoizeCells
                    focusedRowKey={focusedRowKey}
                />
            </ThemeProvider>
        );
        const { rerender } = render(table(null));
        expect(renderType).toHaveBeenCalledTimes(FIELDS.length);
        renderType.mockClear();

        rerender(table('amount'));

        expect(renderType).not.toHaveBeenCalled();
    });
});

describe('Table virtualization', () => {
    const ROW_HEIGHT = 40;
    const manyFields: Field[] = Array.from({ length: 1000 }, (_, i) => ({
        path: `field_${String(i).padStart(4, '0')}`,
        type: 'string',
    }));
    let clientHeight: PropertyDescriptor | undefined;

    beforeEach(() => {
        clientHeight = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'clientHeight');
        Object.defineProperty(HTMLElement.prototype, 'clientHeight', { configurable: true, get: () => 400 });
    });

    afterEach(() => {
        if (clientHeight) Object.defineProperty(HTMLElement.prototype, 'clientHeight', clientHeight);
    });

    const renderVirtualTable = (props: Partial<TableProps<Field>> = {}) =>
        renderTable({
            data: manyFields,
            treeData: undefined,
            isScrollable: true,
            maxHeight: '400px',
            virtualization: { rowHeight: ROW_HEIGHT, overscan: 2 },
            ...props,
        });

    it('renders only the rows in view and moves the window on scroll', () => {
        const { container } = renderVirtualTable();

        // 400px / 40px = 10 visible rows, plus 2 overscan below.
        expect(renderedRows()).toHaveLength(12);
        expect(renderedRows()[0]).toBe('field_0000');

        const scroller = container.firstElementChild as HTMLElement;
        act(() => {
            scroller.scrollTop = 500 * ROW_HEIGHT;
            fireEvent.scroll(scroller);
        });

        expect(renderedRows()).toContain('field_0500');
        expect(renderedRows()).not.toContain('field_0000');
    });

    // A constant config, as callers should pass: the window must still track scrolling once a loading
    // state (when the scroll container isn't mounted) ends.
    const STABLE_VIRTUALIZATION = { rowHeight: ROW_HEIGHT, overscan: 2 };
    const virtualTable = (props: Partial<TableProps<Field>>) => (
        <ThemeProvider theme={themeV2}>
            <Table<Field>
                columns={columns}
                data={manyFields}
                rowKey="path"
                isScrollable
                maxHeight="400px"
                virtualization={STABLE_VIRTUALIZATION}
                {...props}
            />
        </ThemeProvider>
    );

    it('tracks scrolling after a loading state ends', () => {
        const { container, rerender } = render(virtualTable({ isLoading: true }));
        rerender(virtualTable({ isLoading: false }));

        const scroller = container.firstElementChild as HTMLElement;
        act(() => {
            scroller.scrollTop = 500 * ROW_HEIGHT;
            fireEvent.scroll(scroller);
        });

        expect(renderedRows()).toContain('field_0500');
    });

    it('scrolls to the same row again after the key was cleared', () => {
        const { container, rerender } = render(virtualTable({ scrollToRowKey: 'field_0700' }));
        const scroller = container.firstElementChild as HTMLElement;
        scroller.scrollTop = 0;

        rerender(virtualTable({ scrollToRowKey: null }));
        rerender(virtualTable({ scrollToRowKey: 'field_0700' }));

        expect(scroller.scrollTop).toBeGreaterThan(690 * ROW_HEIGHT);
    });

    it('scrolls a row outside the rendered window into view', () => {
        const { container } = renderVirtualTable({ scrollToRowKey: 'field_0700' });

        const scroller = container.firstElementChild as HTMLElement;
        expect(scroller.scrollTop).toBeGreaterThan(690 * ROW_HEIGHT);
        expect(scroller.scrollTop).toBeLessThanOrEqual(700 * ROW_HEIGHT);
    });

    it('keeps its rows when the container is hidden, e.g. in an inactive tab', () => {
        let height = 400;
        Object.defineProperty(HTMLElement.prototype, 'clientHeight', { configurable: true, get: () => height });
        const observers: Array<() => void> = [];
        vi.stubGlobal(
            'ResizeObserver',
            class {
                constructor(callback: () => void) {
                    observers.push(callback);
                }

                observe() {}

                disconnect() {}
            },
        );
        try {
            renderVirtualTable();
            const visibleRows = renderedRows().length;

            height = 0;
            act(() => observers.forEach((callback) => callback()));

            expect(renderedRows()).toHaveLength(visibleRows);
        } finally {
            vi.unstubAllGlobals();
        }
    });

    it('keeps rows at their fixed height and opts out of scroll anchoring', () => {
        const { container } = renderVirtualTable();

        // Rows taller than rowHeight, or anchoring adjusting scrollTop as spacers change, would move the
        // window on every scroll and remount the rows at its edges.
        expect(container.firstElementChild).toHaveStyle({ overflowAnchor: 'none' });
        const cellContent = within(screen.getByTestId('row-field_0000')).getByText('string');
        // The cell's 1px bottom border makes up the rest of the row.
        expect(cellContent).toHaveStyle({ height: `${ROW_HEIGHT - 1}px`, overflow: 'hidden' });
    });

    it('does not re-render cells that stay in view while scrolling', () => {
        const renderType = vi.fn((field: Field) => field.type);
        const spiedColumns: Column<Field>[] = [columns[0], { ...columns[1], render: renderType }];
        const { container } = renderVirtualTable({ columns: spiedColumns });
        // 10 visible rows plus 2 overscan, each rendered once.
        expect(renderType).toHaveBeenCalledTimes(12);
        renderType.mockClear();

        const scroller = container.firstElementChild as HTMLElement;
        act(() => {
            scroller.scrollTop = ROW_HEIGHT;
            fireEvent.scroll(scroller);
        });

        // Only the row that scrolled into the window renders; rows already on screen are reused.
        expect(renderType).toHaveBeenCalledTimes(1);
    });
});
