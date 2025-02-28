import React, { useState } from 'react';
import styled from 'styled-components/macro';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
// import { ExpandedOwner } from '../../../../../components/styled/ExpandedOwner/ExpandedOwner';
import { EMPTY_MESSAGES } from '../../../../../constants';
import { Owner, OwnershipType, OwnershipTypeEntity } from '../../../../../../../../types.generated';
import { useEntityData, useMutationUrn, useRefetch } from '../../../../../../../entity/shared/EntityContext';
import { EditOwnersModal } from '../EditOwnersModal';
import { ENTITY_PROFILE_OWNERS_ID } from '../../../../../../../onboarding/config/EntityProfileOnboardingConfig';
import { OwnershipTypeSection } from './OwnershipTypeSection';
import { getOwnershipTypeName } from '../ownershipUtils';
import { SidebarSection } from '../../SidebarSection';
import SectionActionButton from '../../SectionActionButton';
import EmptySectionText from '../../EmptySectionText';

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
