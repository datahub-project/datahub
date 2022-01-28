import React from 'react';
import { fireEvent, render, waitFor, screen } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import userEvent from '@testing-library/user-event';
// import { useQuery } from '@apollo/client';
import TestPageContainer from '../../../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../../../Mocks';
import { mocks2 } from '../../../../../../MocksCustom';
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
import { FindWhoAmI } from '../../../../dataset/whoAmI';
import { AdminTab } from '../../../tabs/Dataset/Schema/AdminTab';
import { EditSchemaTab } from '../../../tabs/Dataset/Schema/EditSchemaTab';
import { EditPropertiesTab } from '../../../tabs/Dataset/Schema/EditPropertiesTab';
import { EditSampleTab } from '../../../tabs/Dataset/Schema/EditSampleTab';

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
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.upstreamLineage?.entities?.length || 0) > 0 ||
                                        (dataset?.dataset?.downstreamLineage?.entities?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) || false,
                                },
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length && true) ||
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) ||
                                        false,
                                },
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                            {
                                component: SidebarStatsSection,
                                display: {
                                    visible: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length && true) ||
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) ||
                                        false,
                                },
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
        await waitFor(() => expect(getByText('0 upstream, 0 downstream')).toBeInTheDocument());
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
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.upstreamLineage?.entities?.length || 0) > 0 ||
                                        (dataset?.dataset?.downstreamLineage?.entities?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) || false,
                                },
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length && true) ||
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) ||
                                        false,
                                },
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                            {
                                component: SidebarStatsSection,
                                display: {
                                    visible: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length && true) ||
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) ||
                                        false,
                                },
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
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.upstreamLineage?.entities?.length || 0) > 0 ||
                                        (dataset?.dataset?.downstreamLineage?.entities?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) || false,
                                },
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                display: {
                                    enabled: (_, _1) => true,
                                    visible: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length && true) ||
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) ||
                                        false,
                                },
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                            {
                                component: SidebarStatsSection,
                                display: {
                                    visible: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length && true) ||
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) ||
                                        false,
                                },
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

    it('Render edit tabs as authorised data owner', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks2}>
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
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.incoming?.count || 0) > 0 ||
                                        (dataset?.dataset?.outgoing?.count || 0) > 0,
                                },
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.usageStats?.buckets?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length || 0) > 0 ||
                                        (dataset?.dataset?.usageStats?.buckets?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Edit Schema',
                                component: EditSchemaTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        const currUser = FindWhoAmI();
                                        const ownership = _dataset?.dataset?.ownership?.owners;
                                        const ownersArray =
                                            ownership
                                                ?.map((x) =>
                                                    x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser
                                                        ? x?.owner?.urn.split(':').slice(-1)
                                                        : '',
                                                )
                                                .flat() || [];
                                        if (ownersArray.includes(currUser)) {
                                            // console.log('I can see the tab');
                                            return true;
                                        }
                                        return false;
                                    },
                                    enabled: (_, _dataset: GetDatasetQuery) => {
                                        return true;
                                    },
                                },
                            },
                            {
                                name: 'Edit Properties',
                                component: EditPropertiesTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        const currUser = FindWhoAmI();
                                        const ownership = _dataset?.dataset?.ownership?.owners;
                                        const ownersArray =
                                            ownership
                                                ?.map((x) =>
                                                    x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser
                                                        ? x?.owner?.urn.split(':').slice(-1)
                                                        : '',
                                                )
                                                .flat() || [];
                                        if (ownersArray.includes(currUser)) {
                                            return true;
                                        }
                                        return false;
                                    },
                                    enabled: (_, _dataset: GetDatasetQuery) => {
                                        return true;
                                    },
                                },
                            },
                            {
                                name: 'Dataset Admin',
                                component: AdminTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        const currUser = FindWhoAmI();
                                        const ownership = _dataset?.dataset?.ownership?.owners;
                                        const ownersArray =
                                            ownership
                                                ?.map((x) =>
                                                    x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser
                                                        ? x?.owner?.urn.split(':').slice(-1)
                                                        : '',
                                                )
                                                .flat() || [];
                                        if (ownersArray.includes(currUser)) {
                                            return true;
                                        }
                                        return false;
                                    },
                                    enabled: (_, _dataset: GetDatasetQuery) => {
                                        return true;
                                    },
                                },
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                        ]}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        // find the schema fields in the schema table
        // await waitFor(() => expect(getByText('Edit Schema')).toBeInTheDocument())
        await waitFor(() => expect(screen.getAllByText('Edit Schema')).toHaveLength(1));
        await waitFor(() => expect(screen.getAllByText('Dataset Admin')).toHaveLength(1));
        await waitFor(() => expect(screen.getAllByText('Edit Properties')).toHaveLength(1));
        userEvent.click(getByText('Edit Schema'));
        // expect to see schema details in edit tab
        await waitFor(() => expect(screen.getAllByText('STRING')).toHaveLength(2));
        await waitFor(() => expect(screen.getByText('varchar(100)')).toBeInTheDocument());
        userEvent.click(getByText('Add New Row'));
        const editText = screen.getAllByText('Edit');
        console.log(`there are ${editText.length} edit text snippets`);
        await waitFor(() => expect(screen.getAllByText('Edit')).toHaveLength(4));
    });

    it('Render edit properties as authorised data owner', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks2}>
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
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.incoming?.count || 0) > 0 ||
                                        (dataset?.dataset?.outgoing?.count || 0) > 0,
                                },
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.usageStats?.buckets?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length || 0) > 0 ||
                                        (dataset?.dataset?.usageStats?.buckets?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Edit Schema',
                                component: EditSchemaTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        const currUser = FindWhoAmI();
                                        const ownership = _dataset?.dataset?.ownership?.owners;
                                        const ownersArray =
                                            ownership
                                                ?.map((x) =>
                                                    x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser
                                                        ? x?.owner?.urn.split(':').slice(-1)
                                                        : '',
                                                )
                                                .flat() || [];
                                        if (ownersArray.includes(currUser)) {
                                            // console.log('I can see the tab');
                                            return true;
                                        }
                                        return false;
                                    },
                                    enabled: (_, _dataset: GetDatasetQuery) => {
                                        return true;
                                    },
                                },
                            },
                            {
                                name: 'Edit Properties',
                                component: EditPropertiesTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        const currUser = FindWhoAmI();
                                        const ownership = _dataset?.dataset?.ownership?.owners;
                                        const ownersArray =
                                            ownership
                                                ?.map((x) =>
                                                    x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser
                                                        ? x?.owner?.urn.split(':').slice(-1)
                                                        : '',
                                                )
                                                .flat() || [];
                                        if (ownersArray.includes(currUser)) {
                                            return true;
                                        }
                                        return false;
                                    },
                                    enabled: (_, _dataset: GetDatasetQuery) => {
                                        return true;
                                    },
                                },
                            },
                            {
                                name: 'Dataset Admin',
                                component: AdminTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        const currUser = FindWhoAmI();
                                        const ownership = _dataset?.dataset?.ownership?.owners;
                                        const ownersArray =
                                            ownership
                                                ?.map((x) =>
                                                    x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser
                                                        ? x?.owner?.urn.split(':').slice(-1)
                                                        : '',
                                                )
                                                .flat() || [];
                                        if (ownersArray.includes(currUser)) {
                                            return true;
                                        }
                                        return false;
                                    },
                                    enabled: (_, _dataset: GetDatasetQuery) => {
                                        return true;
                                    },
                                },
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                        ]}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        // find the schema fields in the schema table
        // await waitFor(() => expect(getByText('Edit Schema')).toBeInTheDocument())
        await waitFor(() => expect(screen.getAllByText('Edit Schema')).toHaveLength(1));
        await waitFor(() => expect(screen.getAllByText('Dataset Admin')).toHaveLength(1));
        await waitFor(() => expect(screen.getAllByText('Edit Properties')).toHaveLength(1));
        userEvent.click(getByText('Edit Properties'));
        // expect to see properties details in edit tab - there is only 1 property {key: 'propertyAKey', value: 'propertyAValue'}
        await waitFor(() => expect(screen.getAllByText('Edit')).toHaveLength(2));
        await waitFor(() => expect(screen.getByText('propertyAKey')));
        // i am unable to query for the text in the table - it returns me html wrapped text for some reason
        // select the only property and delete it
        userEvent.click(screen.getAllByRole('checkbox')[1]);
        userEvent.click(getByText('Delete Row'));
        await waitFor(() => expect(screen.getAllByText('Edit')).toHaveLength(1));
        // reset changes
        userEvent.click(getByText('Reset Changes'));
        await waitFor(() => expect(screen.getAllByText('Edit')).toHaveLength(2));
    });

    it('Render dataset admin properties as authorised data owner', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks2}>
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
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.incoming?.count || 0) > 0 ||
                                        (dataset?.dataset?.outgoing?.count || 0) > 0,
                                },
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.usageStats?.buckets?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length || 0) > 0 ||
                                        (dataset?.dataset?.usageStats?.buckets?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Edit Schema',
                                component: EditSchemaTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        const currUser = FindWhoAmI();
                                        const ownership = _dataset?.dataset?.ownership?.owners;
                                        const ownersArray =
                                            ownership
                                                ?.map((x) =>
                                                    x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser
                                                        ? x?.owner?.urn.split(':').slice(-1)
                                                        : '',
                                                )
                                                .flat() || [];
                                        if (ownersArray.includes(currUser)) {
                                            // console.log('I can see the tab');
                                            return true;
                                        }
                                        return false;
                                    },
                                    enabled: (_, _dataset: GetDatasetQuery) => {
                                        return true;
                                    },
                                },
                            },
                            {
                                name: 'Edit Properties',
                                component: EditPropertiesTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        const currUser = FindWhoAmI();
                                        const ownership = _dataset?.dataset?.ownership?.owners;
                                        const ownersArray =
                                            ownership
                                                ?.map((x) =>
                                                    x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser
                                                        ? x?.owner?.urn.split(':').slice(-1)
                                                        : '',
                                                )
                                                .flat() || [];
                                        if (ownersArray.includes(currUser)) {
                                            return true;
                                        }
                                        return false;
                                    },
                                    enabled: (_, _dataset: GetDatasetQuery) => {
                                        return true;
                                    },
                                },
                            },
                            {
                                name: 'Dataset Admin',
                                component: AdminTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        const currUser = FindWhoAmI();
                                        const ownership = _dataset?.dataset?.ownership?.owners;
                                        const ownersArray =
                                            ownership
                                                ?.map((x) =>
                                                    x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser
                                                        ? x?.owner?.urn.split(':').slice(-1)
                                                        : '',
                                                )
                                                .flat() || [];
                                        if (ownersArray.includes(currUser)) {
                                            return true;
                                        }
                                        return false;
                                    },
                                    enabled: (_, _dataset: GetDatasetQuery) => {
                                        return true;
                                    },
                                },
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                        ]}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(screen.getAllByText('Dataset Admin')).toHaveLength(1));
        userEvent.click(getByText('Dataset Admin'));
        await waitFor(() => expect(screen.getByText('Deactivate Dataset')).toBeInTheDocument());
        const inputs = screen.getAllByRole('textbox');
        console.log(`there are ${inputs.length} inputs in admin tab`);
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
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.upstreamLineage?.entities?.length || 0) > 0 ||
                                        (dataset?.dataset?.downstreamLineage?.entities?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) || false,
                                },
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                display: {
                                    enabled: (_, _1) => true,
                                    visible: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length && true) ||
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) ||
                                        false,
                                },
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                            {
                                component: SidebarStatsSection,
                                display: {
                                    visible: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length && true) ||
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) ||
                                        false,
                                },
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

    it('renders autorender aspects', async () => {
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
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.upstreamLineage?.entities?.length || 0) > 0 ||
                                        (dataset?.dataset?.downstreamLineage?.entities?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) || false,
                                },
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                display: {
                                    enabled: (_, _1) => true,
                                    visible: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length && true) ||
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) ||
                                        false,
                                },
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                            {
                                component: SidebarStatsSection,
                                display: {
                                    visible: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length && true) ||
                                        (dataset?.dataset?.usageStats?.buckets?.length && true) ||
                                        false,
                                },
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

        // find the tab name
        await waitFor(() => expect(getByText('Auto Render Aspect Custom Tab Name')).toBeInTheDocument());

        // open the custom tab
        fireEvent(
            getByText('Auto Render Aspect Custom Tab Name'),
            new MouseEvent('click', {
                bubbles: true,
                cancelable: true,
            }),
        );

        // find the tab contents
        await waitFor(() => expect(getByText('autoField1')).toBeInTheDocument());
        await waitFor(() => expect(getByText('autoValue1')).toBeInTheDocument());
    });

    it('Render edit samples as owner', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks2}>
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
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.incoming?.count || 0) > 0 ||
                                        (dataset?.dataset?.outgoing?.count || 0) > 0,
                                },
                            },
                            {
                                name: 'Queries',
                                component: QueriesTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.usageStats?.buckets?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Stats',
                                component: StatsTab,
                                display: {
                                    visible: (_, _1) => true,
                                    enabled: (_, dataset: GetDatasetQuery) =>
                                        (dataset?.dataset?.datasetProfiles?.length || 0) > 0 ||
                                        (dataset?.dataset?.usageStats?.buckets?.length || 0) > 0,
                                },
                            },
                            {
                                name: 'Edit Samples',
                                component: EditSampleTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        const currUser = FindWhoAmI();
                                        const ownership = _dataset?.dataset?.ownership?.owners;
                                        const ownersArray =
                                            ownership
                                                ?.map((x) =>
                                                    x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser
                                                        ? x?.owner?.urn.split(':').slice(-1)
                                                        : '',
                                                )
                                                .flat() || [];
                                        if (ownersArray.includes(currUser)) {
                                            // console.log('I can see the tab');
                                            return true;
                                        }
                                        return false;
                                    },
                                    enabled: (_, _dataset: GetDatasetQuery) => {
                                        return true;
                                    },
                                },
                            },
                        ]}
                        sidebarSections={[
                            {
                                component: SidebarAboutSection,
                            },
                        ]}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(screen.getAllByText('Edit Samples')).toHaveLength(1));
        userEvent.click(getByText('Edit Samples'));
        await waitFor(() => expect(screen.getByText('Submit Changes')).toBeInTheDocument());
        await waitFor(() => expect(screen.getByText('Submit Changes').closest('button')).toHaveAttribute('disabled'));
    });
});
