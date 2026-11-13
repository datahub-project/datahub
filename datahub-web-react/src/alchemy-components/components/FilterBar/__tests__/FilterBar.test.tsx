import { fireEvent, render, screen, within } from '@testing-library/react';
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

    it('should show plural operator labels when a chip has multiple values', () => {
        const pluralFields: FilterField[] = [
            {
                field: 'owner',
                label: 'Owner',
                defaultOperator: 'is',
                operators: [
                    { value: 'is', label: 'is', pluralLabel: 'is any of' },
                    { value: 'is_all', label: 'is all of', minValues: 2 },
                    { value: 'is_not', label: 'is not', pluralLabel: 'is not any of' },
                ],
                values: [
                    { value: 'alice', label: 'Alice' },
                    { value: 'bob', label: 'Bob' },
                ],
            },
        ];
        const filterValue: FilterGroup = {
            id: 'root',
            match: 'all',
            filters: [
                {
                    id: 'owner-filter',
                    field: 'owner',
                    operator: 'is',
                    values: ['alice', 'bob'],
                },
            ],
        };
        renderFilterBar(vi.fn(), pluralFields, filterValue);

        expect(screen.getByText('is any of')).toBeInTheDocument();
        expect(screen.queryByText(/^is$/)).not.toBeInTheDocument();
    });

    it('should show the icons of the two named values and how many values are left out, all listed on hover', async () => {
        const platforms = ['Snowflake', 'BigQuery', 'Looker', 'dbt'];
        const iconFields: FilterField[] = [
            {
                field: 'platform',
                label: 'Platform',
                defaultOperator: 'is',
                operators: [{ value: 'is', label: 'is' }],
                values: platforms.map((label) => ({ value: label, label, icon: <img alt={label} /> })),
            },
        ];
        const withValues = (count: number): FilterGroup => ({
            id: 'root',
            match: 'all',
            filters: [{ id: 'platform-filter', field: 'platform', operator: 'is', values: platforms.slice(0, count) }],
        });

        const { unmount } = renderFilterBar(vi.fn(), iconFields, withValues(2));
        expect(screen.getAllByRole('img').map((img) => img.getAttribute('alt'))).toEqual(['Snowflake', 'BigQuery']);
        expect(screen.queryByTestId('filter-bar-value-count')).not.toBeInTheDocument();
        unmount();

        renderFilterBar(vi.fn(), iconFields, withValues(4));
        expect(screen.getAllByRole('img').map((img) => img.getAttribute('alt'))).toEqual(['Snowflake', 'BigQuery']);
        expect(screen.getByText('Snowflake, BigQuery')).toBeInTheDocument();
        expect(screen.getByTestId('filter-bar-value-count')).toHaveTextContent('+2');

        fireEvent.mouseEnter(screen.getByTestId('filter-bar-value').firstElementChild as Element);
        const tooltip = await screen.findByTestId('filter-bar-value-tooltip');
        expect(
            within(tooltip)
                .getAllByRole('img')
                .map((img) => img.getAttribute('alt')),
        ).toEqual(platforms);
    });

    it('should list every selected value on hover when the chip names only some of them', async () => {
        const owners = ['Alice', 'Bob', 'Carol'];
        const ownerFields: FilterField[] = [
            {
                field: 'owner',
                label: 'Owner',
                defaultOperator: 'is',
                operators: [{ value: 'is', label: 'is' }],
                values: owners.map((label) => ({ value: label, label })),
            },
        ];
        renderFilterBar(vi.fn(), ownerFields, {
            id: 'root',
            match: 'all',
            filters: [{ id: 'owner-filter', field: 'owner', operator: 'is', values: owners }],
        });

        expect(screen.getByText('Alice, Bob')).toBeInTheDocument();
        expect(screen.getByTestId('filter-bar-value-count')).toHaveTextContent('+1');
        fireEvent.mouseEnter(screen.getByTestId('filter-bar-value').firstElementChild as Element);

        const tooltip = await screen.findByTestId('filter-bar-value-tooltip');
        owners.forEach((owner) => expect(tooltip).toHaveTextContent(owner));
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

        fireEvent.click(screen.getByRole('tab', { name: 'any' }));

        expect(onChange).toHaveBeenCalledWith({
            ...value,
            match: 'any',
        });
    });

    describe('in a nested group', () => {
        const nestedValue: FilterGroup = {
            id: 'root',
            match: 'all',
            filters: [value.filters[0]],
            groups: [{ id: 'nested', match: 'all', filters: [value.filters[1]] }],
        };

        const renderNested = (onChange: (updatedValue: FilterGroup) => void) => {
            render(
                <CustomThemeProvider>
                    <FilterBar value={nestedValue} fields={fields} onChange={onChange} allowGroups />
                </CustomThemeProvider>,
            );
            // The root group's rule row comes first, then the nested group's.
            return within(screen.getAllByTestId('filter-bar-rules')[1]);
        };

        it("switches the group's match without changing the root's", () => {
            const onChange = vi.fn();
            const nestedRow = renderNested(onChange);

            fireEvent.click(nestedRow.getByRole('tab', { name: 'any' }));

            expect(onChange).toHaveBeenCalledWith({
                ...nestedValue,
                groups: [{ ...nestedValue.groups![0], match: 'any' }],
            });
        });

        it('removes the group from the end of its row', () => {
            const onChange = vi.fn();
            const nestedRow = renderNested(onChange);

            fireEvent.click(nestedRow.getByRole('button', { name: 'Remove group' }));

            expect(onChange).toHaveBeenCalledWith({ ...nestedValue, groups: [] });
        });
    });

    it('should apply the values edited in an open menu when the bar is removed', () => {
        const onChange = vi.fn();
        const multiFields: FilterField[] = [
            {
                ...fields[0],
                selectionMode: 'multiple',
                values: [
                    { value: 'dataset', label: 'Dataset' },
                    { value: 'dashboard', label: 'Dashboard' },
                ],
            },
        ];
        const filterValue: FilterGroup = { id: 'root', match: 'all', filters: [value.filters[0]] };
        const { unmount } = renderFilterBar(onChange, multiFields, filterValue);

        fireEvent.click(screen.getByText('Dataset'));
        fireEvent.click(screen.getByRole('button', { name: /Dashboard/ }));
        expect(onChange).not.toHaveBeenCalled();

        unmount();

        expect(onChange).toHaveBeenCalledWith({
            ...filterValue,
            filters: [{ ...filterValue.filters[0], values: ['dataset', 'dashboard'] }],
        });
    });

    it('should apply multi-selected values when the menu is closed', () => {
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
        expect(onChange).not.toHaveBeenCalled();

        fireEvent.keyDown(document, { key: 'Escape' });

        expect(onChange).toHaveBeenCalledWith({
            ...filterValue,
            filters: [{ ...filterValue.filters[0], values: ['dataset', 'dashboard'] }],
        });
    });

    it('should show a loader in the count slot of uncounted values while counts are loading', () => {
        const countedField = (countsLoading: boolean): FilterField[] => [
            {
                ...fields[0],
                countsLoading,
                values: [
                    { value: 'dataset', label: 'Dataset', count: 100 },
                    { value: 'dashboard', label: 'Dashboard' },
                ],
            },
        ];
        const filterValue: FilterGroup = { id: 'root', match: 'all', filters: [value.filters[0]] };
        const { rerender } = renderFilterBar(vi.fn(), countedField(true), filterValue);
        fireEvent.click(screen.getByText('Dataset'));
        const option = (name: string) => screen.getByTestId(`filter-bar-value-option-${name}`);

        expect(within(option('dataset')).getByTestId('filter-bar-option-count')).toHaveTextContent('100');
        expect(within(option('dataset')).queryByTestId('filter-bar-option-count-loading')).toBeNull();
        expect(within(option('dashboard')).getByTestId('filter-bar-option-count-loading')).toBeInTheDocument();

        rerender(
            <CustomThemeProvider>
                <FilterBar value={filterValue} fields={countedField(false)} onChange={vi.fn()} />
            </CustomThemeProvider>,
        );
        expect(within(option('dashboard')).queryByTestId('filter-bar-option-count-loading')).toBeNull();
        expect(within(option('dashboard')).queryByTestId('filter-bar-option-count')).toBeNull();
    });

    it('should add a chip for a field whose default operator takes no value, without a value flyout', () => {
        const onChange = vi.fn();
        const presenceField: FilterField = {
            field: 'certified',
            label: 'Certified',
            defaultOperator: 'exists',
            operators: [
                { value: 'exists', label: 'exists', requiresValue: false },
                { value: 'not_exists', label: 'does not exist', requiresValue: false },
            ],
        };
        renderFilterBar(onChange, [...fields, presenceField], { id: 'root', match: 'all', filters: [] });
        fireEvent.click(screen.getByTestId('filter-bar-add'));
        fireEvent.mouseEnter(screen.getByTestId('filter-bar-field-certified'));

        expect(screen.queryByTestId('filter-bar-value-menu')).toBeNull();

        fireEvent.click(screen.getByTestId('filter-bar-field-certified'));

        expect(onChange).toHaveBeenCalledWith({
            id: 'root',
            match: 'all',
            filters: [{ id: expect.any(String), field: 'certified', operator: 'exists', values: [] }],
        });
    });

    it('should show a chip with no operator menu when its field offers a single operator', () => {
        const flagField: FilterField = {
            field: 'deprecated',
            label: 'Is Deprecated',
            defaultOperator: 'is_true',
            operators: [{ value: 'is_true', label: '', requiresValue: false }],
        };
        renderFilterBar(vi.fn(), [...fields, flagField], {
            id: 'root',
            match: 'all',
            filters: [{ id: 'f1', field: 'deprecated', operator: 'is_true', values: [] }],
        });

        expect(screen.getByTestId('filter-bar-chip-deprecated')).toHaveTextContent('Is Deprecated');
        expect(screen.queryByTestId('filter-bar-operator')).toBeNull();
        expect(screen.queryByTestId('filter-bar-value')).toBeNull();
    });

    it('should keep the add menu open while composing on the last field it offers', () => {
        function HidingUsedFields() {
            const [group, setGroup] = React.useState<FilterGroup>({ id: 'root', match: 'all', filters: [] });
            const used = new Set(group.filters.map((filter) => filter.field));
            return (
                <CustomThemeProvider>
                    <FilterBar
                        value={group}
                        fields={[
                            { ...fields[0], hidden: true },
                            { ...fields[1], hidden: used.has('domain') },
                        ]}
                        onChange={setGroup}
                    />
                </CustomThemeProvider>
            );
        }
        render(<HidingUsedFields />);
        fireEvent.click(screen.getByTestId('filter-bar-add'));
        fireEvent.mouseEnter(screen.getByTestId('filter-bar-field-domain'));
        fireEvent.click(screen.getByTestId('filter-bar-value-option-finance'));

        expect(screen.getByTestId('filter-bar-chip-domain')).toBeInTheDocument();
        expect(screen.getByTestId('filter-bar-value-menu')).toBeInTheDocument();

        fireEvent.keyDown(document, { key: 'Escape' });

        expect(screen.queryByTestId('filter-bar-add')).toBeNull();
    });

    it('should report when its first menu opens and its last menu closes', () => {
        const onMenuOpenChange = vi.fn();
        render(
            <CustomThemeProvider>
                <FilterBar value={value} fields={fields} onChange={vi.fn()} onMenuOpenChange={onMenuOpenChange} />
            </CustomThemeProvider>,
        );
        fireEvent.click(screen.getByText('Dataset'));
        expect(onMenuOpenChange).toHaveBeenLastCalledWith(true);

        fireEvent.keyDown(document, { key: 'Escape' });
        expect(onMenuOpenChange).toHaveBeenLastCalledWith(false);
        expect(onMenuOpenChange).toHaveBeenCalledTimes(2);
    });

    it('should move focus into the value flyout on Enter or ArrowRight', () => {
        renderFilterBar(vi.fn(), fields, { id: 'root', match: 'all', filters: [] });
        fireEvent.click(screen.getByTestId('filter-bar-add'));
        const typeRow = screen.getByTestId('filter-bar-field-type');
        typeRow.focus();
        screen.getByTestId('filter-bar-field-domain').focus();
        fireEvent.keyDown(typeRow, { key: 'ArrowRight' });

        const valueMenu = screen.getByTestId('filter-bar-value-menu');
        expect(within(valueMenu).getByTestId('filter-bar-value-option-dataset')).toBeInTheDocument();
        expect(valueMenu.contains(document.activeElement)).toBe(true);
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

    it('should reset a server-side value search when the value menu closes', () => {
        const onSearch = vi.fn();
        const searchFields: FilterField[] = [{ ...fields[0], searchable: true, onSearch }];
        renderFilterBar(vi.fn(), searchFields, { id: 'root', match: 'all', filters: [value.filters[0]] });

        fireEvent.click(screen.getByText('Dataset'));
        fireEvent.change(screen.getByTestId('filter-bar-value-search'), { target: { value: 'tab' } });
        expect(onSearch).toHaveBeenLastCalledWith('tab');

        fireEvent.click(screen.getByTestId('filter-bar-value'));
        expect(onSearch).toHaveBeenLastCalledWith('');
    });

    it('should keep a value picked from server-side search results listed after the results change', () => {
        const searchField = (values: FilterField['values']): FilterField[] => [
            { ...fields[0], searchable: true, onSearch: vi.fn(), values },
        ];
        const filterValue: FilterGroup = { id: 'root', match: 'all', filters: [value.filters[0]] };
        const { rerender } = renderFilterBar(
            vi.fn(),
            searchField([{ value: 'dataset', label: 'Dataset' }]),
            filterValue,
        );
        const rerenderWith = (values: FilterField['values']) =>
            rerender(
                <CustomThemeProvider>
                    <FilterBar value={filterValue} fields={searchField(values)} onChange={vi.fn()} />
                </CustomThemeProvider>,
            );

        fireEvent.click(screen.getByText('Dataset'));
        rerenderWith([{ value: 'chart', label: 'Chart' }]);
        fireEvent.click(screen.getByTestId('filter-bar-value-option-chart'));
        rerenderWith([{ value: 'dashboard', label: 'Dashboard' }]);

        expect(screen.getByTestId('filter-bar-value-option-chart')).toBeInTheDocument();
        expect(screen.getByTestId('filter-bar-value-option-dashboard')).toBeInTheDocument();
    });

    it('should allow adding the same field more than once', () => {
        const onChange = vi.fn();
        const filterValue: FilterGroup = {
            id: 'root',
            match: 'all',
            filters: [value.filters[0]],
        };
        renderFilterBar(onChange, fields, filterValue);

        fireEvent.click(screen.getByRole('button', { name: 'Filter' }));
        fireEvent.mouseEnter(screen.getByRole('button', { name: 'Type' }));
        // Commit on checkbox — chip appears without waiting to close the menu.
        const datasetOptions = screen.getAllByRole('button', { name: /Dataset/ });
        fireEvent.click(datasetOptions[datasetOptions.length - 1]);

        expect(onChange).toHaveBeenCalledWith({
            ...filterValue,
            filters: [
                ...filterValue.filters,
                expect.objectContaining({ field: 'type', operator: 'is', values: ['dataset'] }),
            ],
        });
    });

    it('should not add a filter chip when Add Filter closes with no values', () => {
        const onChange = vi.fn();
        const filterValue: FilterGroup = {
            id: 'root',
            match: 'all',
            filters: [value.filters[0]],
        };
        renderFilterBar(onChange, fields, filterValue);

        fireEvent.click(screen.getByRole('button', { name: 'Filter' }));
        fireEvent.mouseEnter(screen.getByRole('button', { name: 'Type' }));
        fireEvent.keyDown(document, { key: 'Escape' });

        expect(onChange).not.toHaveBeenCalled();
    });

    it('should keep a committed chip when hovering a different field', () => {
        const onChange = vi.fn();
        const filterValue: FilterGroup = {
            id: 'root',
            match: 'all',
            filters: [],
        };
        const multiFields: FilterField[] = [
            {
                field: 'owner',
                label: 'Owner',
                defaultOperator: 'is',
                operators: [{ value: 'is', label: 'is', pluralLabel: 'is any of' }],
                values: [
                    { value: 'alice', label: 'Alice' },
                    { value: 'bob', label: 'Bob' },
                ],
            },
            {
                field: 'type',
                label: 'Type',
                defaultOperator: 'is',
                operators: [{ value: 'is', label: 'is' }],
                values: [{ value: 'dataset', label: 'Dataset' }],
            },
        ];
        renderFilterBar(onChange, multiFields, filterValue);

        fireEvent.click(screen.getByRole('button', { name: 'Filter' }));
        fireEvent.mouseEnter(screen.getByRole('button', { name: 'Owner' }));
        fireEvent.click(screen.getByRole('button', { name: /Alice/ }));

        expect(onChange).toHaveBeenCalledWith(
            expect.objectContaining({
                filters: [expect.objectContaining({ field: 'owner', values: ['alice'] })],
            }),
        );

        const ownerCalls = onChange.mock.calls.length;
        fireEvent.mouseEnter(screen.getByRole('button', { name: 'Type' }));

        // Hovering Type must not remove the Owner chip.
        expect(onChange.mock.calls.length).toBe(ownerCalls);
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
                values: [{ value: 'alice', label: 'Alice' }],
            },
        ];
        renderFilterBar(onChange, searchableFields, filterValue);

        fireEvent.click(screen.getByRole('button', { name: 'Filter' }));
        fireEvent.change(screen.getByPlaceholderText('Search filters'), { target: { value: 'owner' } });

        expect(screen.getByRole('button', { name: /^Owner/ })).toBeInTheDocument();
        expect(screen.queryByRole('button', { name: /^Domain/ })).not.toBeInTheDocument();

        fireEvent.mouseEnter(screen.getByRole('button', { name: /^Owner/ }));
        fireEvent.click(screen.getByRole('button', { name: /Alice/ }));

        expect(onChange).toHaveBeenCalledWith({
            ...filterValue,
            filters: [
                ...filterValue.filters,
                expect.objectContaining({ field: 'owner', operator: 'is', values: ['alice'] }),
            ],
        });
    });

    it('should open grouped filter fields on hover', () => {
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
                values: [{ value: 'alice', label: 'Alice' }],
            },
        ];
        renderFilterBar(onChange, searchableFields, filterValue);

        fireEvent.click(screen.getByRole('button', { name: 'Filter' }));

        expect(screen.getByRole('button', { name: 'Type' })).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Governance' })).toBeInTheDocument();
        expect(screen.queryByRole('button', { name: /^Owner/ })).not.toBeInTheDocument();

        fireEvent.mouseEnter(screen.getByRole('button', { name: 'Governance' }));

        expect(screen.getByRole('button', { name: /^Owner/ })).toBeInTheDocument();
        expect(screen.getByRole('button', { name: /^Domain/ })).toBeInTheDocument();

        fireEvent.mouseEnter(screen.getByRole('button', { name: /^Owner/ }));
        fireEvent.click(screen.getByRole('button', { name: /Alice/ }));

        expect(onChange).toHaveBeenCalledWith({
            ...filterValue,
            filters: [
                ...filterValue.filters,
                expect.objectContaining({ field: 'owner', operator: 'is', values: ['alice'] }),
            ],
        });
    });

    it('should render rules on a hidden field but leave the field out of Add Filter', () => {
        renderFilterBar(vi.fn(), [fields[0], { ...fields[1], hidden: true }]);

        expect(screen.getByText('Finance')).toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: 'Filter' }));
        expect(screen.getByRole('button', { name: /^Type/ })).toBeInTheDocument();
        expect(screen.queryByRole('button', { name: /^Domain/ })).not.toBeInTheDocument();
    });

    it('should keep a field listed while its values are composed, after its new rule hides it', () => {
        const filterValue: FilterGroup = { id: 'root', match: 'all', filters: [] };
        const { rerender } = renderFilterBar(vi.fn(), fields, filterValue);

        fireEvent.click(screen.getByRole('button', { name: 'Filter' }));
        fireEvent.mouseEnter(screen.getByRole('button', { name: 'Type' }));
        rerender(
            <CustomThemeProvider>
                <FilterBar
                    value={{ ...filterValue, filters: [value.filters[0]] }}
                    fields={[{ ...fields[0], hidden: true }, fields[1]]}
                    onChange={vi.fn()}
                />
            </CustomThemeProvider>,
        );

        expect(screen.getByTestId('filter-bar-field-type')).toBeInTheDocument();
        expect(screen.getByRole('dialog', { name: 'Type' })).toBeInTheDocument();

        fireEvent.mouseEnter(screen.getByRole('button', { name: 'Domain' }));
        expect(screen.queryByTestId('filter-bar-field-type')).not.toBeInTheDocument();
    });

    it('should put Add Filter after the chips by default', () => {
        renderFilterBar(vi.fn());

        expect(within(screen.getByTestId('filter-bar-rules')).getByTestId('filter-bar-add')).toBeInTheDocument();
        expect(within(screen.getByTestId('filter-bar-actions')).queryByTestId('filter-bar-add')).toBeNull();
    });

    it('should put Add Filter below the chips, before Clear all, when placed below', () => {
        const { rerender } = render(
            <CustomThemeProvider>
                <FilterBar value={value} fields={fields} onChange={vi.fn()} addFilterPlacement="below" />
            </CustomThemeProvider>,
        );

        expect(within(screen.getByTestId('filter-bar-rules')).queryByTestId('filter-bar-add')).toBeNull();
        const actions = within(screen.getByTestId('filter-bar-actions')).getAllByRole('button');
        expect(actions.map((button) => button.getAttribute('data-testid'))).toEqual([
            'filter-bar-add',
            'filter-bar-clear-all',
        ]);

        rerender(
            <CustomThemeProvider>
                <FilterBar
                    value={{ ...value, filters: [] }}
                    fields={fields}
                    onChange={vi.fn()}
                    addFilterPlacement="below"
                />
            </CustomThemeProvider>,
        );
        expect(screen.queryByTestId('filter-bar-rules')).toBeNull();
        expect(screen.getByTestId('filter-bar-add')).toBeInTheDocument();
        expect(screen.queryByTestId('filter-bar-clear-all')).toBeNull();
    });

    it('should open Add Filter above its trigger when there is little room below', () => {
        const rect = vi
            .spyOn(HTMLElement.prototype, 'getBoundingClientRect')
            .mockReturnValue({ top: 700, bottom: 724, left: 10, right: 100, width: 90, height: 24 } as DOMRect);
        try {
            renderFilterBar(vi.fn());
            fireEvent.click(screen.getByTestId('filter-bar-add'));

            const search = screen.getByTestId('filter-bar-field-search');
            const panel = [...document.body.children].find((child) => child.contains(search)) as HTMLElement;
            expect(panel.style.top).toBe('');
            expect(panel.style.bottom).toBe(`${window.innerHeight - 700 + 4}px`);
        } finally {
            rect.mockRestore();
        }
    });

    it('should open its menus beside the trigger, centred on it, when placed to the right', () => {
        const rect = vi
            .spyOn(HTMLElement.prototype, 'getBoundingClientRect')
            .mockReturnValue({ top: 400, bottom: 424, left: 10, right: 100, width: 90, height: 24 } as DOMRect);
        try {
            render(
                <CustomThemeProvider>
                    <FilterBar value={value} fields={fields} onChange={vi.fn()} menuPlacement="right" />
                </CustomThemeProvider>,
            );
            const panelOf = (element: HTMLElement) =>
                [...document.body.children].find((child) => child.contains(element)) as HTMLElement;

            fireEvent.click(screen.getByTestId('filter-bar-add'));
            const addMenu = panelOf(screen.getByTestId('filter-bar-field-search'));
            expect(addMenu.style).toMatchObject({ top: '412px', left: '104px', bottom: '' });
            expect(addMenu.style.maxHeight).toBe(`${window.innerHeight - 24}px`);
            fireEvent.keyDown(document, { key: 'Escape' });

            fireEvent.click(screen.getAllByTestId('filter-bar-operator')[0]);
            const operatorMenu = panelOf(screen.getByTestId('filter-bar-operator-is_not'));
            expect(operatorMenu.style).toMatchObject({ top: '412px', left: '104px' });
        } finally {
            rect.mockRestore();
        }
    });
});
