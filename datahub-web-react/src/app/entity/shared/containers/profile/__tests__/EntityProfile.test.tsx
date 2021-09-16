import React from 'react';
import { fireEvent, render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import TestPageContainer from '../../../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../../../Mocks';
import { EntityProfile } from '../EntityProfile';
import {
    useGetDatasetQuery,
    useUpdateDatasetMutation,
    GetDatasetQuery,
} from '../../../../../../graphql/dataset.generated';
import { EntityType } from '../../../../../../types.generated';
import QueriesTab from '../../../tabs/Dataset/Queries/QueriesTab';
import { SchemaTab } from '../../../tabs/Dataset/Schema/SchemaTab';
import StatsTab from '../../../tabs/Dataset/Stats/StatsTab';
import { DocumentationTab } from '../../../tabs/Documentation/DocumentationTab';
import { LineageTab } from '../../../tabs/Lineage/LineageTab';
import { PropertiesTab } from '../../../tabs/Properties/PropertiesTab';
import { SidebarStatsSection } from '../sidebar/Dataset/StatsSidebarSection';
import { SidebarOwnerSection } from '../sidebar/Ownership/SidebarOwnerSection';
import { SidebarAboutSection } from '../sidebar/SidebarAboutSection';
import { SidebarTagsSection } from '../sidebar/SidebarTagsSection';

describe('EntityProfile', () => {
    it('renders dataset page', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityProfile
                        urn="urn:li:dataset:3"
                        entityType={EntityType.Dataset}
                        useEntityQuery={useGetDatasetQuery}
                        useUpdateQuery={useUpdateDatasetMutation}
                        getOverrideProperties={() => ({})}
                        tabs={[
                            {
                                name: 'Schema',
                                component: SchemaTab,
                            },
                            {
                                name: 'Documentation',
                                component: DocumentationTab,
                            },
                            {
                                name: 'Properties',
                                component: PropertiesTab,
                            },
                            {
                                name: 'Lineage',
                                component: LineageTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    (dataset?.dataset?.upstreamLineage?.entities?.length || 0) === 0 &&
                                    (dataset?.dataset?.downstreamLineage?.entities?.length || 0) === 0,
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.datasetProfiles?.length &&
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                            {
                                component: SidebarStatsSection,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.datasetProfiles?.length &&
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                            {
                                component: SidebarTagsSection,
                            },
                            {
                                component: SidebarOwnerSection,
                            },
                        ]}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(getByText('Yet Another Dataset')).toBeInTheDocument());
        await waitFor(() =>
            expect(getByText('This and here we have yet another Dataset (YAN). Are there more?')).toBeInTheDocument(),
        );
    });

    it('renders tab content', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityProfile
                        urn="urn:li:dataset:3"
                        entityType={EntityType.Dataset}
                        useEntityQuery={useGetDatasetQuery}
                        useUpdateQuery={useUpdateDatasetMutation}
                        getOverrideProperties={() => ({})}
                        tabs={[
                            {
                                name: 'Schema',
                                component: SchemaTab,
                            },
                            {
                                name: 'Documentation',
                                component: DocumentationTab,
                            },
                            {
                                name: 'Properties',
                                component: PropertiesTab,
                            },
                            {
                                name: 'Lineage',
                                component: LineageTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    (dataset?.dataset?.upstreamLineage?.entities?.length || 0) === 0 &&
                                    (dataset?.dataset?.downstreamLineage?.entities?.length || 0) === 0,
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.datasetProfiles?.length &&
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                            {
                                component: SidebarStatsSection,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.datasetProfiles?.length &&
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                            {
                                component: SidebarTagsSection,
                            },
                            {
                                component: SidebarOwnerSection,
                            },
                        ]}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        // find the schema fields in the schema table
        await waitFor(() => expect(getByText('user_name')).toBeInTheDocument());
        await waitFor(() => expect(getByText('user_id')).toBeInTheDocument());
    });

    it('switches tab content', async () => {
        const { getByText, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityProfile
                        urn="urn:li:dataset:3"
                        entityType={EntityType.Dataset}
                        useEntityQuery={useGetDatasetQuery}
                        useUpdateQuery={useUpdateDatasetMutation}
                        getOverrideProperties={() => ({})}
                        tabs={[
                            {
                                name: 'Schema',
                                component: SchemaTab,
                            },
                            {
                                name: 'Documentation',
                                component: DocumentationTab,
                            },
                            {
                                name: 'Properties',
                                component: PropertiesTab,
                            },
                            {
                                name: 'Lineage',
                                component: LineageTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    (dataset?.dataset?.upstreamLineage?.entities?.length || 0) === 0 &&
                                    (dataset?.dataset?.downstreamLineage?.entities?.length || 0) === 0,
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.datasetProfiles?.length &&
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                            {
                                component: SidebarStatsSection,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.datasetProfiles?.length &&
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                            {
                                component: SidebarTagsSection,
                            },
                            {
                                component: SidebarOwnerSection,
                            },
                        ]}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        // find the schema fields in the schema table
        await waitFor(() => expect(getByText('user_name')).toBeInTheDocument());
        await waitFor(() => expect(getByText('user_id')).toBeInTheDocument());
        expect(queryByText('propertyAKey')).not.toBeInTheDocument();

        fireEvent(
            getByText('Properties'),
            new MouseEvent('click', {
                bubbles: true,
                cancelable: true,
            }),
        );

        await waitFor(() => expect(getByText('propertyAKey')).toBeInTheDocument());
        await waitFor(() => expect(getByText('propertyAValue')).toBeInTheDocument());
        expect(queryByText('user_name')).not.toBeInTheDocument();
    });

    it('renders sidebar content', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityProfile
                        urn="urn:li:dataset:3"
                        entityType={EntityType.Dataset}
                        useEntityQuery={useGetDatasetQuery}
                        useUpdateQuery={useUpdateDatasetMutation}
                        getOverrideProperties={() => ({})}
                        tabs={[
                            {
                                name: 'Schema',
                                component: SchemaTab,
                            },
                            {
                                name: 'Documentation',
                                component: DocumentationTab,
                            },
                            {
                                name: 'Properties',
                                component: PropertiesTab,
                            },
                            {
                                name: 'Lineage',
                                component: LineageTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    (dataset?.dataset?.upstreamLineage?.entities?.length || 0) === 0 &&
                                    (dataset?.dataset?.downstreamLineage?.entities?.length || 0) === 0,
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.datasetProfiles?.length &&
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                            {
                                component: SidebarStatsSection,
                                shouldHide: (_, dataset: GetDatasetQuery) =>
                                    !dataset?.dataset?.datasetProfiles?.length &&
                                    !dataset?.dataset?.usageStats?.buckets?.length,
                            },
                            {
                                component: SidebarTagsSection,
                            },
                            {
                                component: SidebarOwnerSection,
                            },
                        ]}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        // find the tags
        await waitFor(() => expect(getByText('Tags')).toBeInTheDocument());
        await waitFor(() => expect(getByText('abc-sample-tag')).toBeInTheDocument());
    });
});
