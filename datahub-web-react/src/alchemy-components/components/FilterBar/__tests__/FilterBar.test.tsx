import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { FilterBar } from '@components/components/FilterBar/FilterBar';
import { FilterField, FilterGroup } from '@components/components/FilterBar/types';

import CustomThemeProvider from '@src/CustomThemeProvider';

const fields: FilterField[] = [
    {
        field: 'type',
        label: 'Type',
        defaultOperator: 'is',
        operators: [
            { value: 'is', label: 'is' },
            { value: 'is_not', label: 'is not' },
        ],
        values: [{ value: 'dataset', label: 'Dataset' }],
    },
    {
        field: 'domain',
        label: 'Domain',
        defaultOperator: 'is',
        operators: [{ value: 'is', label: 'is' }],
        values: [{ value: 'finance', label: 'Finance' }],
    },
];

const value: FilterGroup = {
    id: 'root',
    match: 'all',
    filters: [
        {
            id: 'type-filter',
            field: 'type',
            operator: 'is',
            values: ['dataset'],
        },
        {
            id: 'domain-filter',
            field: 'domain',
            operator: 'is',
            values: ['finance'],
        },
    ],
};

function renderFilterBar(
    onChange: (updatedValue: FilterGroup) => void,
    availableFields: FilterField[] = fields,
    filterValue: FilterGroup = value,
) {
    return render(
        <CustomThemeProvider>
            <FilterBar value={filterValue} fields={availableFields} onChange={onChange} />
        </CustomThemeProvider>,
    );
}

describe('FilterBar', () => {
    it('should render readable field, operator, and value chips', () => {
        renderFilterBar(vi.fn());

        expect(screen.getByText('Type')).toBeInTheDocument();
        expect(screen.getByText('Dataset')).toBeInTheDocument();
        expect(screen.getByText('Domain')).toBeInTheDocument();
        expect(screen.getByText('Finance')).toBeInTheDocument();
    });

    it('should remove a selected filter', () => {
        const onChange = vi.fn();
        renderFilterBar(onChange);

        fireEvent.click(screen.getAllByLabelText('Remove filter')[0]);

        expect(onChange).toHaveBeenCalledWith({
            ...value,
            filters: [value.filters[1]],
        });
    });

    it('should switch between matching all and any filters', () => {
        const onChange = vi.fn();
        renderFilterBar(onChange);

        fireEvent.click(screen.getByLabelText('Where all or any'));

        expect(onChange).toHaveBeenCalledWith({
            ...value,
            match: 'any',
        });
    });

    it('should apply a searched value as soon as it is selected', () => {
        const onChange = vi.fn();
        const onSearch = vi.fn();
        const searchableFields: FilterField[] = [
            {
                ...fields[0],
                searchable: true,
                selectionMode: 'multiple',
                onSearch,
                values: [
                    { value: 'dataset', label: 'Dataset', count: 100 },
                    { value: 'dashboard', label: 'Dashboard', count: 50 },
                ],
            },
        ];
        const filterValue: FilterGroup = {
            id: 'root',
            match: 'all',
            filters: [value.filters[0]],
        };
        renderFilterBar(onChange, searchableFields, filterValue);

        fireEvent.click(screen.getByText('Dataset'));
        fireEvent.change(screen.getByPlaceholderText('Search values'), { target: { value: 'dash' } });
        fireEvent.click(screen.getByRole('button', { name: /Dashboard/ }));

        expect(onSearch).toHaveBeenCalledWith('dash');
        expect(onChange).toHaveBeenCalledWith({
            ...filterValue,
            filters: [{ ...filterValue.filters[0], values: ['dataset', 'dashboard'] }],
        });
    });

    it('should load the next page of values when the list is scrolled to the end', () => {
        const onLoadMore = vi.fn();
        const pagedFields: FilterField[] = [
            {
                ...fields[0],
                selectionMode: 'multiple',
                hasMore: true,
                onLoadMore,
            },
        ];
        const filterValue: FilterGroup = {
            id: 'root',
            match: 'all',
            filters: [value.filters[0]],
        };
        renderFilterBar(vi.fn(), pagedFields, filterValue);

        fireEvent.click(screen.getByText('Dataset'));
        const options = screen.getAllByRole('button', { name: /Dataset/ });
        const list = options[options.length - 1].parentElement as HTMLElement;
        Object.defineProperty(list, 'scrollHeight', { value: 400 });
        Object.defineProperty(list, 'clientHeight', { value: 380 });
        fireEvent.scroll(list);

        expect(onLoadMore).toHaveBeenCalled();
    });

    it('should keep labels for selected values outside the current result page', () => {
        const pagedFields: FilterField[] = [
            {
                ...fields[0],
                values: [],
                selectedOptions: [{ value: 'dataset', label: 'Dataset' }],
            },
        ];
        const filterValue: FilterGroup = {
            id: 'root',
            match: 'all',
            filters: [value.filters[0]],
        };

        renderFilterBar(vi.fn(), pagedFields, filterValue);

        expect(screen.getByText('Dataset')).toBeInTheDocument();
    });

    it('should search the available filter fields', () => {
        const onChange = vi.fn();
        const filterValue: FilterGroup = {
            id: 'root',
            match: 'all',
            filters: [value.filters[0]],
        };
        const searchableFields: FilterField[] = [
            fields[0],
            { ...fields[1], group: 'Governance' },
            {
                field: 'owner',
                label: 'Owner',
                group: 'Governance',
                defaultOperator: 'is',
                operators: [{ value: 'is', label: 'is' }],
                values: [],
            },
        ];
        renderFilterBar(onChange, searchableFields, filterValue);

        fireEvent.click(screen.getByRole('button', { name: 'Filter' }));
        fireEvent.change(screen.getByPlaceholderText('Search filters'), { target: { value: 'owner' } });

        expect(screen.getByRole('button', { name: 'Owner' })).toBeInTheDocument();
        expect(screen.queryByRole('button', { name: 'Domain' })).not.toBeInTheDocument();

        fireEvent.click(screen.getByRole('button', { name: 'Owner' }));

        expect(onChange).toHaveBeenCalledWith({
            ...filterValue,
            filters: [...filterValue.filters, expect.objectContaining({ field: 'owner', operator: 'is', values: [] })],
        });
    });
});
