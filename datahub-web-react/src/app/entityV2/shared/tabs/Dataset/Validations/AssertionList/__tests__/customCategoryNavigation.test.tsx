import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React, { useState } from 'react';
import { useLocation } from 'react-router';
import { describe, expect, it } from 'vitest';

import { buildAssertionListFilters } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionList';
import { ASSERTION_DEFAULT_FILTERS } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/constant';
import { useSetFilterFromURLParams } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/hooks';
import { AssertionListFilter } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { AssertionResultType } from '@types';

function NavigationHarness() {
    const [filters, setFilters] = useState<AssertionListFilter>({
        ...ASSERTION_DEFAULT_FILTERS,
        filterCriteria: {
            ...ASSERTION_DEFAULT_FILTERS.filterCriteria,
            column: ['col_a'],
            status: [AssertionResultType.Failure],
        },
    });
    const location = useLocation();
    useSetFilterFromURLParams(filters, setFilters);
    return (
        <>
            <div data-testid="query">{JSON.stringify(buildAssertionListFilters(filters, ['urn:li:dataset:test']))}</div>
            <div data-testid="search">{location.search}</div>
        </>
    );
}

describe('custom category URL consumption', () => {
    it('decodes the category once, preserves existing filters, and removes only consumed parameters', () => {
        render(
            <MockedProvider>
                <TestPageContainer
                    initialEntries={[
                        '/Quality/List?assertion_type=CUSTOM&assertion_custom_type=Validity%20%2B%20100%25&unrelated=keep',
                    ]}
                >
                    <NavigationHarness />
                </TestPageContainer>
            </MockedProvider>,
        );
        const query = JSON.parse(screen.getByTestId('query').textContent || '[]');
        expect(query[0].and).toEqual(
            expect.arrayContaining([
                expect.objectContaining({ field: 'customType', values: ['Validity + 100%'] }),
                expect.objectContaining({ field: 'fieldPath', values: ['col_a'] }),
                expect.objectContaining({ field: 'assertionStatus', values: ['FAILING'] }),
            ]),
        );
        expect(screen.getByTestId('search')).toHaveTextContent('?unrelated=keep');
    });
});
