import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render } from '@testing-library/react';
import React from 'react';

import { EntityContext } from '@app/entity/shared/EntityContext';
import {
    SidebarDataJobTransformationLogicSection,
    SidebarDatasetViewDefinitionSection,
    SidebarQueryLogicSection,
} from '@app/entityV2/shared/containers/profile/sidebar/SidebarLogicSection';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { dataset3, mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

const datasetWithViewProperties = {
    ...dataset3,
    viewProperties: {
        __typename: 'ViewProperties',
        formattedLogic: 'SELECT\n  *\nFROM\n  table\nWHERE\n  id = 1',
        language: 'SQL',
        logic: 'SELECT * FROM table WHERE id = 1',
        materialized: false,
    },
};

const queryWithProperties = {
    type: EntityType.Query,
    urn: 'urn:li:query:1',
    properties: {
        statement: {
            value: 'SELECT * FROM my_table WHERE id = 123',
        },
        source: 'SYSTEM',
        name: 'Example Query',
        description: 'An example query',
        created: {
            time: 1612396473001,
            actor: 'urn:li:corpuser:datahub',
        },
        createdOn: {
            time: 1612396473001,
            actor: {
                urn: 'urn:li:corpuser:datahub',
                username: 'datahub',
                type: EntityType.CorpUser,
            },
        },
        lastModified: {
            time: 1612396473001,
            actor: 'urn:li:corpuser:datahub',
        },
        origin: {
            type: EntityType.Dataset,
            urn: 'urn:li:dataset:origin-dataset',
            name: 'Origin Dataset',
        },
    },
};

describe('Sidebar Logic Components', () => {
    describe('SidebarDatasetViewDefinitionSection', () => {
        it('does not render when canViewQueries is false', () => {
            const datasetWithoutViewPermission = {
                ...datasetWithViewProperties,
                privileges: {
                    canViewQueries: false,
                },
            };

            const { container } = render(
                <MockedProvider mocks={mocks} addTypename={false}>
                    <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                        <EntityContext.Provider
                            value={{
                                urn: 'urn:li:dataset:3',
                                entityType: EntityType.Dataset,
                                entityData: getDataForEntityType({
                                    data: datasetWithoutViewPermission,
                                    entityType: EntityType.Dataset,
                                    getOverrideProperties: () => ({}),
                                }),
                                baseEntity: { dataset: datasetWithoutViewPermission },
                                updateEntity: vi.fn(),
                                routeToTab: vi.fn(),
                                refetch: vi.fn(),
                                lineage: undefined,
                                loading: false,
                                dataNotCombinedWithSiblings: null,
                            }}
                        >
                            <SidebarDatasetViewDefinitionSection />
                        </EntityContext.Provider>
                    </TestPageContainer>
                </MockedProvider>,
            );

            expect(container.firstChild).toBeNull();
        });

        it('renders view definition section when canViewQueries is true', () => {
            const datasetWithViewPermission = {
                ...datasetWithViewProperties,
                privileges: {
                    canViewQueries: true,
                },
            };

            const { getByText } = render(
                <MockedProvider mocks={mocks} addTypename={false}>
                    <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                        <EntityContext.Provider
                            value={{
                                urn: 'urn:li:dataset:3',
                                entityType: EntityType.Dataset,
                                entityData: getDataForEntityType({
                                    data: datasetWithViewPermission,
                                    entityType: EntityType.Dataset,
                                    getOverrideProperties: () => ({}),
                                }),
                                baseEntity: { dataset: datasetWithViewPermission },
                                updateEntity: vi.fn(),
                                routeToTab: vi.fn(),
                                refetch: vi.fn(),
                                lineage: undefined,
                                loading: false,
                                dataNotCombinedWithSiblings: null,
                            }}
                        >
                            <SidebarDatasetViewDefinitionSection />
                        </EntityContext.Provider>
                    </TestPageContainer>
                </MockedProvider>,
            );

            expect(getByText('View Definition')).toBeInTheDocument();
        });

        it('renders view definition section', () => {
            const { getByText } = render(
                <MockedProvider mocks={mocks} addTypename={false}>
                    <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                        <EntityContext.Provider
                            value={{
                                urn: 'urn:li:dataset:3',
                                entityType: EntityType.Dataset,
                                entityData: getDataForEntityType({
                                    data: datasetWithViewProperties,
                                    entityType: EntityType.Dataset,
                                    getOverrideProperties: () => ({}),
                                }),
                                baseEntity: { dataset: datasetWithViewProperties },
                                updateEntity: vi.fn(),
                                routeToTab: vi.fn(),
                                refetch: vi.fn(),
                                lineage: undefined,
                                loading: false,
                                dataNotCombinedWithSiblings: null,
                            }}
                        >
                            <SidebarDatasetViewDefinitionSection />
                        </EntityContext.Provider>
                    </TestPageContainer>
                </MockedProvider>,
            );

            expect(getByText('View Definition')).toBeInTheDocument();
        });

        it('handles format switching', () => {
            const { getByText } = render(
                <MockedProvider mocks={mocks} addTypename={false}>
                    <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                        <EntityContext.Provider
                            value={{
                                urn: 'urn:li:dataset:3',
                                entityType: EntityType.Dataset,
                                entityData: getDataForEntityType({
                                    data: datasetWithViewProperties,
                                    entityType: EntityType.Dataset,
                                    getOverrideProperties: () => ({}),
                                }),
                                baseEntity: { dataset: datasetWithViewProperties },
                                updateEntity: vi.fn(),
                                routeToTab: vi.fn(),
                                refetch: vi.fn(),
                                lineage: undefined,
                                loading: false,
                                dataNotCombinedWithSiblings: null,
                            }}
                        >
                            <SidebarDatasetViewDefinitionSection />
                        </EntityContext.Provider>
                    </TestPageContainer>
                </MockedProvider>,
            );

            const rawButton = getByText('Raw');
            fireEvent.click(rawButton);
            expect(getByText('Raw')).toBeInTheDocument();

            const formattedButton = getByText('Formatted');
            fireEvent.click(formattedButton);
            expect(getByText('Formatted')).toBeInTheDocument();
        });
    });

    describe('SidebarQueryLogicSection', () => {
        it('renders query logic section', () => {
            const { getByText } = render(
                <MockedProvider mocks={mocks} addTypename={false}>
                    <TestPageContainer initialEntries={['/query/urn:li:query:1']}>
                        <EntityContext.Provider
                            value={{
                                urn: 'urn:li:query:1',
                                entityType: EntityType.Query,
                                entityData: getDataForEntityType({
                                    data: queryWithProperties,
                                    entityType: EntityType.Query,
                                    getOverrideProperties: () => ({}),
                                }),
                                baseEntity: { entity: queryWithProperties },
                                updateEntity: vi.fn(),
                                routeToTab: vi.fn(),
                                refetch: vi.fn(),
                                lineage: undefined,
                                loading: false,
                                dataNotCombinedWithSiblings: null,
                            }}
                        >
                            <SidebarQueryLogicSection />
                        </EntityContext.Provider>
                    </TestPageContainer>
                </MockedProvider>,
            );

            expect(getByText('Logic')).toBeInTheDocument();
        });

        it('handles modal interaction', () => {
            const { getByText, queryByRole } = render(
                <MockedProvider mocks={mocks} addTypename={false}>
                    <TestPageContainer initialEntries={['/query/urn:li:query:1']}>
                        <EntityContext.Provider
                            value={{
                                urn: 'urn:li:query:1',
                                entityType: EntityType.Query,
                                entityData: getDataForEntityType({
                                    data: queryWithProperties,
                                    entityType: EntityType.Query,
                                    getOverrideProperties: () => ({}),
                                }),
                                baseEntity: { entity: queryWithProperties },
                                updateEntity: vi.fn(),
                                routeToTab: vi.fn(),
                                refetch: vi.fn(),
                                lineage: undefined,
                                loading: false,
                                dataNotCombinedWithSiblings: null,
                            }}
                        >
                            <SidebarQueryLogicSection />
                        </EntityContext.Provider>
                    </TestPageContainer>
                </MockedProvider>,
            );

            const seeFullButton = getByText('See Full');
            fireEvent.click(seeFullButton);
            expect(queryByRole('dialog')).toBeInTheDocument();

            const closeButton = getByText('Close');
            fireEvent.click(closeButton);
            expect(queryByRole('dialog')).not.toBeInTheDocument();
        });

        it('does not render when no query properties are present', () => {
            const queryWithoutProperties = {
                ...queryWithProperties,
                properties: null,
            };

            const { container } = render(
                <MockedProvider mocks={mocks} addTypename={false}>
                    <TestPageContainer initialEntries={['/query/urn:li:query:1']}>
                        <EntityContext.Provider
                            value={{
                                urn: 'urn:li:query:1',
                                entityType: EntityType.Query,
                                entityData: getDataForEntityType({
                                    data: queryWithoutProperties,
                                    entityType: EntityType.Query,
                                    getOverrideProperties: () => ({}),
                                }),
                                baseEntity: { entity: queryWithoutProperties },
                                updateEntity: vi.fn(),
                                routeToTab: vi.fn(),
                                refetch: vi.fn(),
                                lineage: undefined,
                                loading: false,
                                dataNotCombinedWithSiblings: null,
                            }}
                        >
                            <SidebarQueryLogicSection />
                        </EntityContext.Provider>
                    </TestPageContainer>
                </MockedProvider>,
            );

            expect(container.firstChild).toBeNull();
        });
    });

    describe('SidebarDataJobTransformationLogicSection', () => {
        const dataJobWithLogic = {
            type: EntityType.DataJob,
            urn: 'urn:li:dataJob:test',
            dataTransformLogic: {
                transforms: [
                    {
                        queryStatement: {
                            value: 'SELECT * FROM input_table',
                        },
                    },
                ],
            },
        };

        it('does not render when canViewQueries is false', () => {
            const dataJobWithoutViewPermission = {
                ...dataJobWithLogic,
                privileges: {
                    canViewQueries: false,
                },
            };

            const { container } = render(
                <MockedProvider mocks={mocks} addTypename={false}>
                    <TestPageContainer initialEntries={['/dataJob/urn:li:dataJob:test']}>
                        <EntityContext.Provider
                            value={{
                                urn: 'urn:li:dataJob:test',
                                entityType: EntityType.DataJob,
                                entityData: getDataForEntityType({
                                    data: dataJobWithoutViewPermission,
                                    entityType: EntityType.DataJob,
                                    getOverrideProperties: () => ({}),
                                }),
                                baseEntity: { dataJob: dataJobWithoutViewPermission },
                                updateEntity: vi.fn(),
                                routeToTab: vi.fn(),
                                refetch: vi.fn(),
                                lineage: undefined,
                                loading: false,
                                dataNotCombinedWithSiblings: null,
                            }}
                        >
                            <SidebarDataJobTransformationLogicSection />
                        </EntityContext.Provider>
                    </TestPageContainer>
                </MockedProvider>,
            );

            expect(container.firstChild).toBeNull();
        });

        it('renders logic section when canViewQueries is true', () => {
            const dataJobWithViewPermission = {
                ...dataJobWithLogic,
                privileges: {
                    canViewQueries: true,
                },
            };

            const { getByText } = render(
                <MockedProvider mocks={mocks} addTypename={false}>
                    <TestPageContainer initialEntries={['/dataJob/urn:li:dataJob:test']}>
                        <EntityContext.Provider
                            value={{
                                urn: 'urn:li:dataJob:test',
                                entityType: EntityType.DataJob,
                                entityData: getDataForEntityType({
                                    data: dataJobWithViewPermission,
                                    entityType: EntityType.DataJob,
                                    getOverrideProperties: () => ({}),
                                }),
                                baseEntity: { dataJob: dataJobWithViewPermission },
                                updateEntity: vi.fn(),
                                routeToTab: vi.fn(),
                                refetch: vi.fn(),
                                lineage: undefined,
                                loading: false,
                                dataNotCombinedWithSiblings: null,
                            }}
                        >
                            <SidebarDataJobTransformationLogicSection />
                        </EntityContext.Provider>
                    </TestPageContainer>
                </MockedProvider>,
            );

            expect(getByText('Logic')).toBeInTheDocument();
        });

        it('renders logic section when privileges is undefined (backward compatibility)', () => {
            const { getByText } = render(
                <MockedProvider mocks={mocks} addTypename={false}>
                    <TestPageContainer initialEntries={['/dataJob/urn:li:dataJob:test']}>
                        <EntityContext.Provider
                            value={{
                                urn: 'urn:li:dataJob:test',
                                entityType: EntityType.DataJob,
                                entityData: getDataForEntityType({
                                    data: dataJobWithLogic,
                                    entityType: EntityType.DataJob,
                                    getOverrideProperties: () => ({}),
                                }),
                                baseEntity: { dataJob: dataJobWithLogic },
                                updateEntity: vi.fn(),
                                routeToTab: vi.fn(),
                                refetch: vi.fn(),
                                lineage: undefined,
                                loading: false,
                                dataNotCombinedWithSiblings: null,
                            }}
                        >
                            <SidebarDataJobTransformationLogicSection />
                        </EntityContext.Provider>
                    </TestPageContainer>
                </MockedProvider>,
            );

            expect(getByText('Logic')).toBeInTheDocument();
        });
    });
});
