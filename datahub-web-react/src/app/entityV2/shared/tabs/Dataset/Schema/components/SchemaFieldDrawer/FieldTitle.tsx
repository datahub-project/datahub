import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

const FieldName = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.WHITE_WIRE};
    font-size: 16px;
    font-weight: 700;
    line-height: 24px;
    overflow: hidden;
    display: block;
    cursor: pointer;
    :hover {
        font-weight: bold;
    }
`;

interface Props {
    displayName: string;
}

export default function FieldTitle({ displayName }: Props) {
    const name = displayName.split('.').pop();
    return <FieldName>{name}</FieldName>;
}
