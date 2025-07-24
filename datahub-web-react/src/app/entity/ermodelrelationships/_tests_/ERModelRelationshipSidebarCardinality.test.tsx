import { ApolloClient, ApolloProvider, InMemoryCache } from '@apollo/client';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import ERModelRelationshipSidebarCardinality from '@app/entity/ermodelrelationships/profile/sidebar/ERModelRelationshipSidebarCardinality';
import { EntityContext } from '@app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { Dataset, EntityType, ErModelRelationshipCardinality } from '@src/types.generated';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

// Create a mock Apollo Client
const mockApolloClient = new ApolloClient({
    cache: new InMemoryCache(),
    uri: 'https://mock-api.com/graphql', // Mock URI
});

const mockEntityData: GenericEntityProperties = {
    urn: 'urn:li:ermodelrelationship:123',
    properties: {
        cardinality: ErModelRelationshipCardinality.OneN,
        destination: {
            urn: 'urn:li:dataset:test-dataset-destination',
            name: 'Test Dataset Destination',
        } as Dataset,
        source: {
            urn: 'urn:li:dataset:test-dataset-source',
            name: 'Test Dataset Source',
        } as Dataset,
        name: 'Test Relationship',
    },
    type: EntityType.ErModelRelationship,
} as GenericEntityProperties;

describe('ERModelRelationshipSidebarCardinality', () => {
    it('renders the sidebar header', () => {
        render(
            <ApolloProvider client={mockApolloClient}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            entityData: mockEntityData,
                            urn: 'urn:li:ermodelrelationship:123',
                            entityType: EntityType.ErModelRelationship,
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: false,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <ERModelRelationshipSidebarCardinality />
                    </EntityContext.Provider>
                </TestPageContainer>
            </ApolloProvider>,
        );

        expect(screen.getByText('Cardinality')).toBeInTheDocument();
        expect(screen.getByText(ErModelRelationshipCardinality.OneN)).toBeInTheDocument();
    });
});
