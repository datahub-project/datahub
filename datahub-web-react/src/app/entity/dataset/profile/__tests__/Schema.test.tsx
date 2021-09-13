import React from 'react';
import { fireEvent, render } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { sampleSchema, sampleSchemaWithTags } from '../stories/sampleSchema';
import { mocks } from '../../../../../Mocks';
import { SchemaTab } from '../../../shared/tabs/Dataset/Schema/SchemaTab';
import EntityContext from '../../../shared/EntityContext';
import { EntityType, SchemaMetadata } from '../../../../../types.generated';

describe('Schema', () => {
    it('renders', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                description: 'This is a description',
                                schemaMetadata: sampleSchema as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: jest.fn(),
                            routeToTab: jest.fn(),
                            refetch: jest.fn(),
                        }}
                    >
                        <SchemaTab />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('name')).toBeInTheDocument();
        expect(getByText('the name of the order')).toBeInTheDocument();
        expect(getByText('shipping_address')).toBeInTheDocument();
        expect(getByText('the address the order ships to')).toBeInTheDocument();
    });

    it('renders raw', () => {
        const { getByText, queryAllByTestId } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                description: 'This is a description',
                                schemaMetadata: sampleSchema as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: jest.fn(),
                            routeToTab: jest.fn(),
                            refetch: jest.fn(),
                        }}
                    >
                        <SchemaTab />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(queryAllByTestId('schema-raw-view')).toHaveLength(0);

        const rawButton = getByText('Raw');
        fireEvent.click(rawButton);

        expect(queryAllByTestId('schema-raw-view')).toHaveLength(1);

        const schemaButton = getByText('Tabular');
        fireEvent.click(schemaButton);

        expect(queryAllByTestId('schema-raw-view')).toHaveLength(0);
    });

    it('renders tags and terms', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                description: 'This is a description',
                                schemaMetadata: sampleSchemaWithTags as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: jest.fn(),
                            routeToTab: jest.fn(),
                            refetch: jest.fn(),
                        }}
                    >
                        <SchemaTab />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Legacy')).toBeInTheDocument();
        expect(getByText('sample-glossary-term')).toBeInTheDocument();
    });

    it('renders description', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                description: 'This is a description',
                                schemaMetadata: sampleSchemaWithTags as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: jest.fn(),
                            routeToTab: jest.fn(),
                            refetch: jest.fn(),
                        }}
                    >
                        <SchemaTab />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('order id')).toBeInTheDocument();
    });

    it('renders field', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                description: 'This is a description',
                                schemaMetadata: sampleSchemaWithTags as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: jest.fn(),
                            routeToTab: jest.fn(),
                            refetch: jest.fn(),
                        }}
                    >
                        <SchemaTab />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('shipping_address')).toBeInTheDocument();
    });
});
