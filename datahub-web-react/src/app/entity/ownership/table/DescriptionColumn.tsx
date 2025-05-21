import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { OwnershipTypeEntity } from '@types';

const DescriptionText = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 400;
`;

type Props = {
    ownershipType: OwnershipTypeEntity;
};

export const DescriptionColumn = ({ ownershipType }: Props) => {
    const description = ownershipType?.info?.description || '';

    return <DescriptionText>{description}</DescriptionText>;
};
