import React from 'react';
import { render, waitFor, screen } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import userEvent from '@testing-library/user-event';
import TestPageContainer from '../../../../../../utils/test-utils/TestPageContainer';
import { EntityProfile } from '../EntityProfile';
import {
    useGetDatasetQuery,
    useUpdateDatasetMutation,
    GetDatasetQuery,
} from '../../../../../../graphql/dataset.generated';
import { EntityType } from '../../../../../../types.generated';
import { SidebarAboutSection } from '../sidebar/SidebarAboutSection';
import { EditSchemaTab } from '../../../tabs/Dataset/Schema/EditSchemaTab';
import { CheckOwnership } from '../../../../dataset/whoAmI';
import { EditPropertiesTab } from '../../../tabs/Dataset/PropertiesEdit/EditPropertiesTab';
import { AdminTab } from '../../../tabs/Dataset/Schema/AdminTab';
import { editMocks } from '../../../../../../MocksCustom';

describe('EntityProfile Edit', () => {
    it('Render edit tabs as authorised data owner', async () => {
        const { getByText } = render(
            <MockedProvider mocks={editMocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityProfile
                        urn="urn:li:dataset:3"
                        entityType={EntityType.Dataset}
                        useEntityQuery={useGetDatasetQuery}
                        useUpdateQuery={useUpdateDatasetMutation}
                        getOverrideProperties={() => ({})}
                        tabs={[
                            {
                                name: 'Edit Schema',
                                component: EditSchemaTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        return CheckOwnership(_dataset);
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
        await waitFor(() => expect(screen.getAllByText('Edit Schema')).toHaveLength(1));
        userEvent.click(getByText('Edit Schema'));
        // expect to see schema details in edit tab
        await waitFor(() => expect(screen.getAllByText('STRING')).toHaveLength(2));
        await waitFor(() => expect(screen.getByText('varchar(100)')).toBeInTheDocument());
        userEvent.click(getByText('Add New Row'));
        await waitFor(() => expect(screen.getAllByText('Edit')).toHaveLength(4));
    });
    it('Render edit properties as authorised data owner', async () => {
        const { getByText } = render(
            <MockedProvider mocks={editMocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityProfile
                        urn="urn:li:dataset:3"
                        entityType={EntityType.Dataset}
                        useEntityQuery={useGetDatasetQuery}
                        useUpdateQuery={useUpdateDatasetMutation}
                        getOverrideProperties={() => ({})}
                        tabs={[
                            {
                                name: 'Edit Properties',
                                component: EditPropertiesTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        return CheckOwnership(_dataset);
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
            <MockedProvider mocks={editMocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityProfile
                        urn="urn:li:dataset:3"
                        entityType={EntityType.Dataset}
                        useEntityQuery={useGetDatasetQuery}
                        useUpdateQuery={useUpdateDatasetMutation}
                        getOverrideProperties={() => ({})}
                        tabs={[
                            {
                                name: 'Dataset Administration',
                                component: AdminTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        return CheckOwnership(_dataset);
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

        await waitFor(() => expect(screen.getAllByText('Dataset Administration')).toHaveLength(1));
        userEvent.click(getByText('Dataset Administration'));
        userEvent.click(getByText('Soft Delete Dataset'));
        await waitFor(() => expect(screen.getByText('Deactivate Dataset')).toBeInTheDocument());
    });
    it('Render dataset admin properties - show existing name', async () => {
        const { getByText } = render(
            <MockedProvider mocks={editMocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityProfile
                        urn="urn:li:dataset:3"
                        entityType={EntityType.Dataset}
                        useEntityQuery={useGetDatasetQuery}
                        useUpdateQuery={useUpdateDatasetMutation}
                        getOverrideProperties={() => ({})}
                        tabs={[
                            {
                                name: 'Dataset Administration',
                                component: AdminTab,
                                display: {
                                    visible: (_, _dataset: GetDatasetQuery) => {
                                        return CheckOwnership(_dataset);
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

        await waitFor(() => expect(screen.getAllByText('Dataset Administration')).toHaveLength(1));
        userEvent.click(getByText('Dataset Administration'));
        userEvent.click(getByText('Edit Dataset Container'));
        // should see more than 1 instance of container name - the default place next to the platform icon
        // const display = screen.getByTestId("select");
        // console.log(`display is ${display.innerText}`);
        // suspect container name is not shown cos the entityregistry is not correctly configured.
        await waitFor(() => expect(screen.getAllByTestId('parentContainerTestId')).toHaveLength(1));
    });
});
