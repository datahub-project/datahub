import { Typography, Button } from 'antd';
import React, { useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';
import { ExpandedOwner } from '../../../../components/styled/ExpandedOwner';
import { EMPTY_MESSAGES } from '../../../../constants';
import { useEntityData, useMutationUrn, useRefetch } from '../../../../EntityContext';
import { SidebarHeader } from '../SidebarHeader';
import { AddOwnersModal } from './AddOwnersModal';

export const SidebarOwnerSection = ({ properties }: { properties?: any }) => {
    const { entityType, entityData } = useEntityData();
    const mutationUrn = useMutationUrn();

    const refetch = useRefetch();
    const [showAddModal, setShowAddModal] = useState(false);
    const ownersEmpty = !entityData?.ownership?.owners?.length;

    return (
        <div>
            <SidebarHeader title="Owners" />
            <div>
                {entityData?.ownership?.owners?.map((owner) => (
                    <ExpandedOwner
                        key={owner.owner.urn}
                        entityUrn={owner.associatedUrn || mutationUrn}
                        owner={owner}
                        refetch={refetch}
                    />
                ))}
                {ownersEmpty && (
                    <Typography.Paragraph type="secondary">
                        {EMPTY_MESSAGES.owners.title}. {EMPTY_MESSAGES.owners.description}
                    </Typography.Paragraph>
                )}

                <Button type={ownersEmpty ? 'default' : 'text'} onClick={() => setShowAddModal(true)}>
                    <PlusOutlined /> Add Owners
                </Button>
            </div>
            {showAddModal && (
                <AddOwnersModal
                    urn={mutationUrn}
                    defaultOwnerType={properties?.defaultOwnerType}
                    hideOwnerType={properties?.hideOwnerType || false}
                    type={entityType}
                    refetch={refetch}
                    onCloseModal={() => {
                        setShowAddModal(false);
                    }}
                />
            )}
        </div>
    );
};
