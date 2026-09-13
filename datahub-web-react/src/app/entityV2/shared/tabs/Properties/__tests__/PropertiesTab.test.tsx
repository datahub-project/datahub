import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { PropertiesTab } from '@app/entityV2/shared/tabs/Properties/PropertiesTab';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const mockUseStructuredProperties = vi.fn();

vi.mock('@app/entityV2/shared/tabs/Properties/useStructuredProperties', () => ({
    default: () => mockUseStructuredProperties(),
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({ entityData: null }),
}));

vi.mock('@app/entityV2/shared/tabs/Properties/useHydratedEntityMap', () => ({
    useHydratedEntityMap: () => ({}),
}));

vi.mock('@app/shared/Loading', () => ({
    default: () => <div data-testid="loading-indicator" />,
}));

const hookState = (loading: boolean) => ({
    structuredPropertyRows: [],
    expandedRowsFromFilter: new Set(),
    structuredPropertyRowsRaw: [],
    loading,
});

describe('PropertiesTab field-properties loading state', () => {
    afterEach(() => {
        vi.clearAllMocks();
    });

    it('shows the loading indicator instead of an empty table while field properties are pending', () => {
        mockUseStructuredProperties.mockReturnValue(hookState(true));
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <PropertiesTab properties={{ fieldPath: 'fieldA', disableSearch: true }} />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.getByTestId('loading-indicator')).toBeInTheDocument();
    });

    it('renders the table once field properties finish loading', () => {
        mockUseStructuredProperties.mockReturnValue(hookState(false));
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <PropertiesTab properties={{ fieldPath: 'fieldA', disableSearch: true }} />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.queryByTestId('loading-indicator')).not.toBeInTheDocument();
        expect(screen.getByTestId('entity-properties-table')).toBeInTheDocument();
    });
});
