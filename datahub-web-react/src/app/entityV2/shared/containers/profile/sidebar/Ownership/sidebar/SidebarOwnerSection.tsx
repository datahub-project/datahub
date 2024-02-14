import React, { useState } from 'react';
import { Typography } from 'antd';
import styled from 'styled-components/macro';
import { PlusOutlined } from '@ant-design/icons';
// import { ExpandedOwner } from '../../../../../components/styled/ExpandedOwner/ExpandedOwner';
import { EMPTY_MESSAGES } from '../../../../../constants';
import { Owner, OwnershipType, OwnershipTypeEntity } from '../../../../../../../../types.generated';
import { useEntityData, useMutationUrn, useRefetch } from '../../../../../EntityContext';
import { EditOwnersModal } from '../EditOwnersModal';
import { ENTITY_PROFILE_OWNERS_ID } from '../../../../../../../onboarding/config/EntityProfileOnboardingConfig';
import { OwnershipTypeSection } from './OwnershipTypeSection';
import { getOwnershipTypeName } from '../ownershipUtils';
import { SidebarSection } from '../../SidebarSection';

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
`;

const EmptyText = styled(Typography.Text)`
    && {
        margin-right: 12px;
    }
`;

const AddButton = styled.div`
    margin: 0px;
    padding: 0px;
    :hover {
        cursor: pointer;
    }
`;

const StyledPlusOutlined = styled(PlusOutlined)`
    && {
        font-size: 10px;
        margin-right: 6px;
    }
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
                        {ownersEmpty && <EmptyText type="secondary">{EMPTY_MESSAGES.owners.title}.</EmptyText>}
                        {!readOnly && (
                            <AddButton data-testid="add-owners-button" onClick={() => setShowAddModal(true)}>
                                <StyledPlusOutlined /> Add owners
                            </AddButton>
                        )}
                    </Content>
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
