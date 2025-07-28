import { ApolloClient, ApolloProvider, InMemoryCache } from '@apollo/client';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { ERModelRelationPreview } from '@app/entity/shared/components/styled/ERModelRelationship/ERModelRelationPreview';

import { Dataset, EntityType, ErModelRelationship, ErModelRelationshipCardinality } from '@types';

// Create a mock Apollo Client
const mockApolloClient = new ApolloClient({
    cache: new InMemoryCache(),
    uri: 'https://mock-api.com/graphql', // Mock URI
});

const mockErModelRelationData: ErModelRelationship = {
    id: '123',
    urn: 'urn:li:ermodelrelationship:123',
    properties: {
        cardinality: ErModelRelationshipCardinality.OneN,
        relationshipFieldMappings: [
            { sourceField: 'sourceField1', destinationField: 'destinationField1' },
            { sourceField: 'sourceField2', destinationField: 'destinationField2' },
        ],
        source: {
            urn: 'urn:li:dataset:source-dataset',
            name: 'Source Dataset',
        } as Dataset,
        destination: {
            urn: 'urn:li:dataset:destination-dataset',
            name: 'Destination Dataset',
        } as Dataset,
        name: 'Test Relationship',
    },
    editableProperties: {
        name: 'Editable Test Relationship',
        description: 'This is a test description for the relationship.',
    },
    type: EntityType.ErModelRelationship,
} as ErModelRelationship;

const mockRefetch = vi.fn();

// Corrected Mock for EntityRegistry
vi.mock('@app/entity/EntityRegistry', async () => {
    const actual = await vi.importActual('@app/entity/EntityRegistry');
    return {
        ...(actual as object),
        default: vi.fn().mockImplementation(() => ({
            getEntityName: vi.fn((entityType) => {
                if (entityType === EntityType.ErModelRelationship) {
                    return 'ER Model Relationship';
                }
                return 'Unknown Entity';
            }),
            getEntityUrl: vi.fn((entityType, urn) => `/entity/${entityType}/${urn}`),
        })),
    };
});

describe('ERModelRelationPreview', () => {
    it('renders the relationship header and description', () => {
        render(
            <ApolloProvider client={mockApolloClient}>
                <ERModelRelationPreview
                    ermodelrelationData={mockErModelRelationData}
                    prePageType="Dataset"
                    refetch={mockRefetch}
                />
            </ApolloProvider>,
        );

        expect(screen.getByText('Editable Test Relationship')).toBeInTheDocument();
        expect(screen.getByText('This is a test description for the relationship.')).toBeInTheDocument();
    });

    it('renders the cardinality', () => {
        render(
            <ApolloProvider client={mockApolloClient}>
                <ERModelRelationPreview
                    ermodelrelationData={mockErModelRelationData}
                    prePageType="Dataset"
                    refetch={mockRefetch}
                />
            </ApolloProvider>,
        );

        expect(screen.getByText('Cardinality:')).toBeInTheDocument();
        expect(screen.getByText(ErModelRelationshipCardinality.NOne)).toBeInTheDocument();
    });

    it('renders the field mappings table', () => {
        render(
            <ApolloProvider client={mockApolloClient}>
                <ERModelRelationPreview
                    ermodelrelationData={mockErModelRelationData}
                    prePageType="Dataset"
                    refetch={mockRefetch}
                />
            </ApolloProvider>,
        );

        expect(screen.getByText('sourceField1')).toBeInTheDocument();
        expect(screen.getByText('destinationField1')).toBeInTheDocument();
        expect(screen.getByText('sourceField2')).toBeInTheDocument();
        expect(screen.getByText('destinationField2')).toBeInTheDocument();
    });

    it('renders the "View dataset" buttons', () => {
        render(
            <ApolloProvider client={mockApolloClient}>
                <ERModelRelationPreview
                    ermodelrelationData={mockErModelRelationData}
                    prePageType="Dataset"
                    refetch={mockRefetch}
                />
            </ApolloProvider>,
        );

        const viewSourceDatasetButton = screen.getByText('View dataset');
        const viewDestinationDatasetButton = screen.getByText('View dataset');

        expect(viewSourceDatasetButton).toBeInTheDocument();
        expect(viewDestinationDatasetButton).toBeInTheDocument();
    });
});
