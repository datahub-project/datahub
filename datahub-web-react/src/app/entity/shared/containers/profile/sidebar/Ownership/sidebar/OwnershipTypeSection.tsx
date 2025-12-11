/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { ExpandedOwner } from '@app/entity/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { getOwnershipTypeName } from '@app/entity/shared/containers/profile/sidebar/Ownership/ownershipUtils';

import { Owner, OwnershipTypeEntity } from '@types';

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
