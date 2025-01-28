import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components/macro';
import { OwnershipTypeEntity } from '../../../../types.generated';

const NameText = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 700;
`;

type Props = {
    ownershipType: OwnershipTypeEntity;
};

export const NameColumn = ({ ownershipType }: Props) => {
    const name = ownershipType?.info?.name || ownershipType?.urn;

    return <NameText>{name}</NameText>;
};
