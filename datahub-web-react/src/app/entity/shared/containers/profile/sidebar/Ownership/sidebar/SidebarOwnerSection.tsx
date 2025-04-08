import { PlusOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
// import { ExpandedOwner } from '../../../../../components/styled/ExpandedOwner/ExpandedOwner';
import { EMPTY_MESSAGES } from '@app/entity/shared/constants';
import { EditOwnersModal } from '@app/entity/shared/containers/profile/sidebar/Ownership/EditOwnersModal';
import { getOwnershipTypeName } from '@app/entity/shared/containers/profile/sidebar/Ownership/ownershipUtils';
import { OwnershipTypeSection } from '@app/entity/shared/containers/profile/sidebar/Ownership/sidebar/OwnershipTypeSection';
import { SidebarHeader } from '@app/entity/shared/containers/profile/sidebar/SidebarHeader';
import { ENTITY_PROFILE_OWNERS_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';

import { Owner, OwnershipType, OwnershipTypeEntity } from '@types';

interface Props {
    properties?: any;
    readOnly?: boolean;
}

export const SidebarOwnerSection = ({ properties, readOnly }: Props) => {
    const { entityType, entityData } = useEntityData();
    const mutationUrn = useMutationUrn();

    const refetch = useRefetch();
    const [showAddModal, setShowAddModal] = useState(false);
    const ownersEmpty = !entityData?.ownership?.owners?.length;
    const ownershipTypesMap: Map<string, OwnershipTypeEntity> = new Map();
    const ownersByTypeMap: Map<string, Owner[]> = new Map();
    entityData?.ownership?.owners?.forEach((owner) => {
        const ownershipType = owner?.ownershipType;
        const ownershipTypeName = getOwnershipTypeName(ownershipType);
        // If ownership type is not in the map, add it
        if (ownershipType && !ownershipTypesMap.has(ownershipTypeName)) {
            ownershipTypesMap.set(ownershipTypeName, ownershipType);
        }
        if (!ownersByTypeMap.has(ownershipTypeName)) {
            ownersByTypeMap.set(ownershipTypeName, []);
        }
        ownersByTypeMap.get(ownershipTypeName)?.push(owner);
    });
    // Sort ownership types by name alphabetically
    const ownershipTypeNames = Array.from(ownershipTypesMap.keys()).sort();

    let defaultOwnerTypeUrn = 'urn:li:ownershipType:__system__technical_owner';
    switch (properties?.defaultOwnerType) {
        case OwnershipType.TechnicalOwner:
            defaultOwnerTypeUrn = 'urn:li:ownershipType:__system__technical_owner';
            break;
        case OwnershipType.BusinessOwner:
            defaultOwnerTypeUrn = 'urn:li:ownershipType:__system__business_owner';
            break;
        case OwnershipType.DataSteward:
            defaultOwnerTypeUrn = 'urn:li:ownershipType:__system__data_steward';
            break;
        default:
            break;
    }

    return (
        <div id={ENTITY_PROFILE_OWNERS_ID}>
            <SidebarHeader title="Owners" />
            <div>
                {ownershipTypeNames.map((ownershipTypeName) => {
                    const ownershipType = ownershipTypesMap.get(ownershipTypeName) as OwnershipTypeEntity;
                    const owners = ownersByTypeMap.get(ownershipTypeName) as Owner[];
                    return <OwnershipTypeSection ownershipType={ownershipType} owners={owners} readOnly={readOnly} />;
                })}
                {ownersEmpty && (
                    <Typography.Paragraph type="secondary">
                        {EMPTY_MESSAGES.owners.title}. {EMPTY_MESSAGES.owners.description}
                    </Typography.Paragraph>
                )}
                {!readOnly && (
                    <Button
                        type={ownersEmpty ? 'default' : 'text'}
                        onClick={() => setShowAddModal(true)}
                        data-testid="add-owners-button"
                    >
                        <PlusOutlined /> Add Owners
                    </Button>
                )}
            </div>
            {showAddModal && (
                <EditOwnersModal
                    urns={[mutationUrn]}
                    defaultOwnerType={defaultOwnerTypeUrn}
                    hideOwnerType={properties?.hideOwnerType || false}
                    entityType={entityType}
                    refetch={refetch}
                    onCloseModal={() => {
                        setShowAddModal(false);
                    }}
                />
            )}
        </div>
    );
};
