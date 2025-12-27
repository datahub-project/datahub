import { EditOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useState } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { EditOrganizationDetailsModal } from '@app/organization/EditOrganizationDetailsModal';
import { Organization } from '@app/types.generated';

export const OrganizationEditButton = () => {
    const { entityData } = useEntityData();
    const organization = entityData as Organization;
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);

    const entityUrn = organization?.urn || '';
    const entityName = organization?.properties?.name || '';
    const entityDescription = organization?.properties?.description || null;

    return (
        <>
            <Button type="text" icon={<EditOutlined />} onClick={() => setIsEditModalVisible(true)}>
                Edit
            </Button>
            {isEditModalVisible && (
                <EditOrganizationDetailsModal
                    urn={entityUrn}
                    name={entityName}
                    description={entityDescription}
                    visible={isEditModalVisible}
                    onClose={() => setIsEditModalVisible(false)}
                />
            )}
        </>
    );
};
