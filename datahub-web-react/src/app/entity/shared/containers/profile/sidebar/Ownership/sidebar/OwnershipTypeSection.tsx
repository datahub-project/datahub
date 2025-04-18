import React from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { Owner, OwnershipTypeEntity } from '../../../../../../../../types.generated';
import { ExpandedOwner } from '../../../../../components/styled/ExpandedOwner/ExpandedOwner';
import { useMutationUrn, useRefetch } from '../../../../../EntityContext';
import { getOwnershipTypeName } from '../ownershipUtils';

const OwnershipTypeContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-bottom: 16px;
`;

const OwnershipTypeNameText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-weight: 500;
    font-size: 10px;
    line-height: 14px;
    color: #434343;
`;

const OwnersContainer = styled.div`
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    margin-top: 8px;
    gap: 6px;
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
    return (
        <OwnershipTypeContainer>
            <OwnershipTypeNameText>{ownershipTypeName}</OwnershipTypeNameText>
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
