import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components/macro';
import { OwnershipTypeEntity } from '../../../../types.generated';

const DescriptionText = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 400;
`;

type Props = {
    ownershipType: OwnershipTypeEntity;
};

export const DescriptionColumn = ({ ownershipType }: Props) => {
    const description = ownershipType?.info?.description || '';

    return <DescriptionText>{description}</DescriptionText>;
};
