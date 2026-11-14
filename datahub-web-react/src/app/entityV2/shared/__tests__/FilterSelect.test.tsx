import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { FilterSelect } from '@app/entityV2/shared/FilterSelect';
import TestPageContainer from '@utils/test-utils/TestPageContainer';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

const CATEGORY = { name: 'SUCCESS', category: 'category', count: 2, displayName: 'SUCCESS' };
const STATUS = { name: 'SUCCESS', category: 'status', count: 3, displayName: 'Passing' };

function renderFilters(selectedFilter: typeof CATEGORY, onFilterChange: ReturnType<typeof vi.fn>) {
    return (
        <MockedProvider>
            <TestPageContainer>
                <FilterSelect
                    filterOptions={{ category: [CATEGORY], status: [STATUS] }}
                    initialSelectedOptions={[
                        {
                            value: selectedFilter.name,
                            label: selectedFilter.displayName,
                            parentValue: selectedFilter.category,
                        },
                    ]}
                    onFilterChange={onFilterChange}
                    shouldDisplayConfirmationFooter={false}
                />
            </TestPageContainer>
        </MockedProvider>
    );
}

describe('FilterSelect', () => {
    it('selects, removes, and syncs same-named filters independently across groups', async () => {
        mockVisibilityObserver();
        const onFilterChange = vi.fn();
        const { rerender } = render(renderFilters(CATEGORY, onFilterChange));

        await userEvent.click(screen.getByTestId('filter-base'));
        await userEvent.click(screen.getByText('Status'));
        await userEvent.click(screen.getByText('Passing'));
        expect(onFilterChange).toHaveBeenLastCalledWith([CATEGORY, STATUS]);

        await userEvent.click(screen.getByText('Passing'));
        expect(onFilterChange).toHaveBeenLastCalledWith([CATEGORY]);

        rerender(renderFilters(STATUS, onFilterChange));
        expect(onFilterChange).toHaveBeenLastCalledWith([STATUS]);

        await userEvent.click(screen.getByText('Passing'));
        expect(onFilterChange).toHaveBeenLastCalledWith([]);
    });
});
