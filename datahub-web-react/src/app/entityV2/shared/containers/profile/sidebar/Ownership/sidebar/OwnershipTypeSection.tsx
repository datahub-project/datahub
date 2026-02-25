import { Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { ExpandedOwner } from '@app/entityV2/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { getOwnershipTypeDescription } from '@app/entityV2/shared/components/styled/ExpandedOwner/OwnerUtils';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { getOwnershipTypeName } from '@app/entityV2/shared/containers/profile/sidebar/Ownership/ownershipUtils';

import { Owner, OwnershipTypeEntity } from '@types';

const OwnershipTypeContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-right 12px;
    max-width: inherit;
`;

const OwnershipTypeNameText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-weight: 500;
    font-size: 10px;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

const OwnersContainer = styled.div`
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    margin-top: 8px;
    max-width: inherit;
    gap: 8px;
`;

interface Props {
    ownershipType: OwnershipTypeEntity;
    owners: Owner[];
    readOnly?: boolean;
}

export const OwnershipTypeSection = ({ ownershipType, owners, readOnly }: Props) => {
    const mutationUrn = useMutationUrn();
    const refetch = useRefetch();
    const ownershipTypeName = getOwnershipTypeName(ownershipType);
    const ownershipTypeDescription = getOwnershipTypeDescription(ownershipType);
    return (
        <OwnershipTypeContainer>
            <Tooltip title={ownershipTypeDescription}>
                <OwnershipTypeNameText>{ownershipTypeName}</OwnershipTypeNameText>
            </Tooltip>
            <OwnersContainer>
                {owners.map((owner) => (
                    <ExpandedOwner
                        key={owner.owner.urn}
                        entityUrn={owner.associatedUrn || mutationUrn}
                        owner={owner}
                        refetch={refetch}
                        readOnly={readOnly}
                    />
                ))}
            </OwnersContainer>
        </OwnershipTypeContainer>
    );
};
