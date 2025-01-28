import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render } from '@testing-library/react';
import React from 'react';
import { dataset3, mocks } from '../../../../../../../Mocks';
import { EntityType } from '../../../../../../../types.generated';
import TestPageContainer from '../../../../../../../utils/test-utils/TestPageContainer';
import { EntityContext } from '../../../../../../entity/shared/EntityContext';
import { getDataForEntityType } from '../../utils';
import { SidebarDatasetViewDefinitionSection, SidebarQueryLogicSection } from '../SidebarLogicSection';

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

            const dismissButton = getByText('Dismiss');
            fireEvent.click(dismissButton);
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
});
