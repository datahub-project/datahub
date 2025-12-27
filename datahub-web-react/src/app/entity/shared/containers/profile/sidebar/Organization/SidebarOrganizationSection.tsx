import { EditOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData, useMutationUrn } from '@app/entity/shared/EntityContext';
import { EditOrganizationModal } from '@app/entity/shared/containers/profile/sidebar/Organization/EditOrganizationModal';
import { SidebarHeader } from '@app/entity/shared/containers/profile/sidebar/SidebarHeader';
import { EntityType } from '@app/types.generated';
import { useEntityRegistry } from '@app/useEntityRegistry';

const OrganizationList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 0 20px 20px 20px;
`;

const OrganizationItem = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

export const SidebarOrganizationSection = () => {
    const { entityData } = useEntityData();
    const urn = useMutationUrn();
    const entityRegistry = useEntityRegistry();
    const [isModalVisible, setIsModalVisible] = useState(false);

    const organizations = entityData?.organizations || [];

    return (
        <div>
            <SidebarHeader title="Organizations" />
            <OrganizationList>
                {organizations.map((org: any) => (
                    <OrganizationItem key={org.urn}>
                        <Link to={entityRegistry.getEntityUrl(EntityType.Organization, org.urn)}>
                            {entityRegistry.getDisplayName(EntityType.Organization, org)}
                        </Link>
                    </OrganizationItem>
                ))}
                {organizations.length === 0 && <Typography.Text type="secondary">No organizations</Typography.Text>}
                <Button
                    type="link"
                    icon={<EditOutlined />}
                    onClick={() => setIsModalVisible(true)}
                    style={{ paddingLeft: 0 }}
                >
                    Edit
                </Button>
            </OrganizationList>
            {isModalVisible && (
                <EditOrganizationModal
                    visible={isModalVisible}
                    onClose={() => setIsModalVisible(false)}
                    urn={urn}
                    initialOrganizations={organizations}
                />
            )}
        </div>
    );
};
