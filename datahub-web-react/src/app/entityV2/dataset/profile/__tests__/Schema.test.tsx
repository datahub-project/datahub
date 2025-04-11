import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render } from '@testing-library/react';
import React from 'react';
import { mocks } from '../../../../../Mocks';
import { EntityType, SchemaMetadata } from '../../../../../types.generated';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { EntityContext } from '../../../../entity/shared/EntityContext';
import { SchemaTab } from '../../../shared/tabs/Dataset/Schema/SchemaTab';
import { TabRenderType } from '../../../shared/types';
import SchemaRow from '../schema/components/SchemaRow';
import {
    sampleSchema,
    sampleSchemaWithKeyValueFields,
    sampleSchemaWithoutFields,
    sampleSchemaWithPkFk,
    sampleSchemaWithTags,
} from '../stories/sampleSchema';

vi.mock('virtualizedtableforantd4', async () => {
    return {
        ...(await vi.importActual<object>('virtualizedtableforantd4')),
        useVT: () => [{ body: { row: SchemaRow } }, vi.fn()],
    };
});

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
                                schemaMetadata: sampleSchema as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <SchemaTab renderType={TabRenderType.DEFAULT} />
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
                                properties: {
                                    description: 'This is a description',
                                },
                                schemaMetadata: sampleSchema as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <SchemaTab renderType={TabRenderType.DEFAULT} />
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
                                properties: {
                                    description: 'This is a description',
                                },
                                schemaMetadata: sampleSchemaWithTags as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <SchemaTab renderType={TabRenderType.DEFAULT} />
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
                                properties: {
                                    description: 'This is a description',
                                },
                                schemaMetadata: sampleSchemaWithTags as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <SchemaTab renderType={TabRenderType.DEFAULT} />
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
                                properties: {
                                    description: 'This is a description',
                                },
                                schemaMetadata: sampleSchemaWithTags as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <SchemaTab renderType={TabRenderType.DEFAULT} />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('shipping_address')).toBeInTheDocument();
    });

    it('renders primary keys', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                properties: {
                                    description: 'This is a description',
                                },
                                schemaMetadata: sampleSchemaWithPkFk as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <SchemaTab renderType={TabRenderType.DEFAULT} />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Primary Key')).toBeInTheDocument();
    });

    it.skip('renders foreign keys', () => {
        const { getByText, getAllByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                properties: {
                                    description: 'This is a description',
                                },
                                schemaMetadata: sampleSchemaWithPkFk as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <SchemaTab renderType={TabRenderType.DEFAULT} />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Foreign Key')).toBeInTheDocument();

        const fkButton = getByText('Foreign Key');
        fireEvent.click(fkButton);

        expect(getByText('Foreign Key to')).toBeInTheDocument();
        expect(getAllByText('Yet Another Dataset')).toHaveLength(2);
    });

    it('renders key/value toggle', () => {
        const { getByText, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                properties: {
                                    description: 'This is a description',
                                },
                                schemaMetadata: sampleSchemaWithKeyValueFields as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <SchemaTab renderType={TabRenderType.DEFAULT} />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Key')).toBeInTheDocument();
        expect(getByText('Value')).toBeInTheDocument();
        expect(getByText('count')).toBeInTheDocument();
        expect(getByText('cost')).toBeInTheDocument();
        expect(queryByText('id')).not.toBeInTheDocument();

        const keyButton = getByText('Key');
        fireEvent.click(keyButton);

        expect(getByText('Key')).toBeInTheDocument();
        expect(getByText('Value')).toBeInTheDocument();
        expect(getByText('id')).toBeInTheDocument();
        expect(queryByText('count')).not.toBeInTheDocument();
        expect(queryByText('cost')).not.toBeInTheDocument();
    });

    it('does not renders key/value toggle when no schema', () => {
        const { queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                properties: {
                                    description: 'This is a description',
                                },
                                schemaMetadata: sampleSchemaWithoutFields as SchemaMetadata,
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <SchemaTab renderType={TabRenderType.DEFAULT} />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(queryByText('Key')).not.toBeInTheDocument();
        expect(queryByText('Value')).not.toBeInTheDocument();
    });

    it('renders usage column when usage is present', () => {
        const usageStats = {
            buckets: [
                {
                    bucket: Date.now(),
                    metrics: {
                        totalSqlQueries: 10,
                    },
                },
            ],
            aggregations: {
                uniqueUserCount: 2,
                totalSqlQueries: 10,
                fields: [
                    {
                        fieldName: 'id',
                        count: 10,
                    },
                    {
                        fieldName: 'name',
                        count: 24,
                    },
                ],
            },
        };

        const { queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                properties: {
                                    description: 'This is a description',
                                },
                                schemaMetadata: sampleSchema as SchemaMetadata,
                            },
                            baseEntity: {
                                dataset: {
                                    urn: 'urn:li:dataset:123',
                                    usageStats,
                                },
                            },
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <SchemaTab renderType={TabRenderType.DEFAULT} />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(queryByText('Stats')).toBeInTheDocument();
    });
});
