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
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

export const EntityCountText = styled(Typography.Text)`
    display: inline-block;
    font-size: 12px;
    line-height: 20px;
    font-weight: 400;
    color: ${ANTD_GRAY[7]};
`;

interface Props {
    entityCount?: number;
    displayAssetsText?: boolean;
}

function EntityCount(props: Props) {
    const { entityCount, displayAssetsText } = props;

    if (!entityCount || entityCount <= 0) return <EntityCountText className="entityCount">0 assets</EntityCountText>;

    return (
        <EntityCountText className="entityCount">
            {entityCount.toLocaleString()}{' '}
            {displayAssetsText ? (
                <>{entityCount === 1 ? 'asset' : 'assets'}</>
            ) : (
                <>{entityCount === 1 ? 'entity' : 'entities'}</>
            )}
        </EntityCountText>
    );
}

export default EntityCount;
