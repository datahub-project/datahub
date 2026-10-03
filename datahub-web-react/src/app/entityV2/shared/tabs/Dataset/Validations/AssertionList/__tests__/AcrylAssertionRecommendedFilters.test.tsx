import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { AcrylAssertionRecommendedFilters } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionRecommendedFilters';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('recommended assertion filters', () => {
    it('retains only the selected category when a zero-count status has the same name', () => {
        const selected = { name: 'SUCCESS', displayName: 'Custom SUCCESS', category: 'category', count: 0 };
        const onFilterChange = vi.fn();
        render(
            <MockedProvider>
                <TestPageContainer>
                    <AcrylAssertionRecommendedFilters
                        filters={[selected, { name: 'SUCCESS', displayName: 'Passing', category: 'status', count: 0 }]}
                        appliedFilters={[selected]}
                        onFilterChange={onFilterChange}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.queryByText('Passing')).not.toBeInTheDocument();
        fireEvent.click(screen.getByText('Custom SUCCESS'));
        expect(onFilterChange).toHaveBeenCalledWith([]);
    });
});
