import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { Modal, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import { AddOrganizationModal } from '@app/entityV2/shared/containers/profile/sidebar/Organizations/AddOrganizationModal';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import {
    useAddEntityToOrganizationsMutation,
    useRemoveEntityFromOrganizationsMutation,
} from '@graphql/mutations.generated';
import OrganizationPill from '@app/sharedV2/badges/OrganizationPill';

const Content = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    flex-wrap: wrap;
    gap: 8px;
`;

interface Props {
    readOnly?: boolean;
}

export const SidebarOrganizationSection = ({ readOnly }: Props) => {
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const urn = useMutationUrn();
    const [showModal, setShowModal] = useState(false);
    const [addEntityToOrganizations] = useAddEntityToOrganizationsMutation();
    const [removeEntityFromOrganizations] = useRemoveEntityFromOrganizationsMutation();

    const organizations = entityData?.organizations || [];

    const handleRemoveOrganization = async (orgUrn: string, orgName: string) => {
        try {
            await removeEntityFromOrganizations({
                variables: {
                    entityUrn: urn,
                    organizationUrns: [orgUrn],
                },
            });
            message.success({ content: `Removed from ${orgName}`, duration: 2 });
            refetch?.();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to remove organization: \n ${e.message || ''}`, duration: 3 });
            }
        }
    };

    const handleAddOrganizations = async (orgUrns: string[]) => {
        try {
            await addEntityToOrganizations({
                variables: {
                    entityUrn: urn,
                    organizationUrns: orgUrns,
                },
            });
            message.success({ content: 'Added to organizations', duration: 2 });
            refetch?.();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to add organizations: \n ${e.message || ''}`, duration: 3 });
            }
        }
    };

    const onRemoveClick = (orgUrn: string, orgName: string) => (e) => {
        e.preventDefault();
        Modal.confirm({
            title: `Remove from ${orgName}?`,
            content: `Are you sure you want to remove this entity from ${orgName}?`,
            onOk() {
                handleRemoveOrganization(orgUrn, orgName);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <div className="sidebar-organization-section">
            <SidebarSection
                title="Organizations"
                content={
                    <Content>
                        {organizations.map((org: any) => {
                            const displayName = org.properties?.name || org.urn.split(':').pop() || org.urn;
                            return (
                                <OrganizationPill
                                    key={org.urn}
                                    organizationUrn={org.urn}
                                    organizationName={displayName}
                                    onClose={!readOnly ? onRemoveClick(org.urn, displayName) : undefined}
                                />
                            );
                        })}
                        {organizations.length === 0 && <EmptySectionText message="No organizations" />}
                    </Content>
                }
                extra={
                    <SectionActionButton
                        button={organizations.length > 0 ? <EditOutlinedIcon /> : <AddRoundedIcon />}
                        onClick={(event) => {
                            setShowModal(true);
                            event.stopPropagation();
                        }}
                        actionPrivilege={!readOnly}
                        dataTestId="edit-organizations-button"
                    />
                }
            />
            <AddOrganizationModal open={showModal} onClose={() => setShowModal(false)} onAdd={handleAddOrganizations} />
        </div>
    );
};
