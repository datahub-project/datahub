import { UserOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import OwnerDetail from '@app/entityV2/domain/summary/OwnerDetail';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { getOwnershipTypeName } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils';
import { SummaryTabHeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';

import { Owner, OwnershipTypeEntity } from '@types';

const OwnershipTypeNameText = styled(Typography.Text)`
    font-weight: 500;
    font-size: 12px;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

const OwnersContainer = styled.div`
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    gap: 8px;
`;

const OwnershipContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex-wrap: wrap;
    gap: 8px;
`;

const SectionContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

const Details = styled.div`
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    gap: 24px;
`;

const OwnersSection = () => {
    const { entityData } = useEntityData();
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

    if (ownersEmpty) return null;

    return (
        <SectionContainer>
            <SummaryTabHeaderTitle title="Owners" icon={<UserOutlined />} />
            <Details>
                {ownershipTypeNames.map((ownershipTypeName) => {
                    const owners = ownersByTypeMap.get(ownershipTypeName) as Owner[];
                    return (
                        <OwnershipContainer>
                            <OwnershipTypeNameText>{ownershipTypeName}</OwnershipTypeNameText>
                            <OwnersContainer>
                                {owners.map((owner) => (
                                    <OwnerDetail owner={owner} />
                                ))}
                            </OwnersContainer>
                        </OwnershipContainer>
                    );
                })}
            </Details>
        </SectionContainer>
    );
};

export default OwnersSection;
