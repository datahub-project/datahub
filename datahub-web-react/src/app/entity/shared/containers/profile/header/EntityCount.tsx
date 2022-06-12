import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';

export const EntityCountText = styled(Typography.Text)`
    display: inline-block;
    font-size: 12px;
    line-height: 20px;
    font-weight: 400;
    color: ${ANTD_GRAY[7]};
`;

interface Props {
    entityCount?: number;
}

function EntityCount(props: Props) {
    const { entityCount } = props;

    if (!entityCount || entityCount <= 0) return null;

    return (
        <EntityCountText className="entityCount">
            {entityCount.toLocaleString()} {entityCount === 1 ? 'entity' : 'entities'}
        </EntityCountText>
    );
}

export default EntityCount;
