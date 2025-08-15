import AddRoundedIcon from '@mui/icons-material/AddRounded';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
// import { ExpandedOwner } from '../../../../../components/styled/ExpandedOwner/ExpandedOwner';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import { EditOwnersModal } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/EditOwnersModal';
import { getOwnershipTypeName } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils';
import { OwnershipTypeSection } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/sidebar/OwnershipTypeSection';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { ENTITY_PROFILE_OWNERS_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';

import { Owner, OwnershipType, OwnershipTypeEntity } from '@types';

const Content = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    flex-wrap: wrap;
`;

const OwnershipSections = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
    max-width: 100%;
`;

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

    const canEditOwners = !!entityData?.privileges?.canEditOwners;

    return (
        <div id={ENTITY_PROFILE_OWNERS_ID}>
            <SidebarSection
                title="Owners"
                content={
                    <Content>
                        <OwnershipSections>
                            {ownershipTypeNames.map((ownershipTypeName) => {
                                const ownershipType = ownershipTypesMap.get(ownershipTypeName) as OwnershipTypeEntity;
                                const owners = ownersByTypeMap.get(ownershipTypeName) as Owner[];
                                return (
                                    <OwnershipTypeSection
                                        key={ownershipTypeName}
                                        ownershipType={ownershipType}
                                        owners={owners}
                                        readOnly={readOnly}
                                    />
                                );
                            })}
                        </OwnershipSections>
                        {ownersEmpty && <EmptySectionText message={EMPTY_MESSAGES.owners.title} />}
                    </Content>
                }
                extra={
                    !readOnly && (
                        <SectionActionButton
                            button={<AddRoundedIcon />}
                            onClick={(event) => {
                                setShowAddModal(true);
                                event.stopPropagation();
                            }}
                            actionPrivilege={canEditOwners}
                            dataTestId="addOwner"
                        />
                    )
                }
            />
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
