import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';

import FilterOption from '@app/searchV2/filters/FilterOption';
import { TYPE_NAMES_FILTER_NAME } from '@app/searchV2/utils/constants';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('FilterOption', () => {
    it('shows sub-type labels capitalized', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <FilterOption
                        filterOption={{ field: TYPE_NAMES_FILTER_NAME, value: 'external table', count: 2 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={vi.fn()}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText(/^External table/)).toBeInTheDocument();
    });
});
