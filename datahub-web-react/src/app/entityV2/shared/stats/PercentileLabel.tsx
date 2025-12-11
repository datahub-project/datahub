/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Popover } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { percentileToColor, percentileToLabel } from '@app/entityV2/shared/stats/statsUtils';

const Description = styled.div`
    color: white;
    font-size: 14px;
`;

const Label = styled.span<{ color?: string }>`
    border-radius: 8px;
    padding: 2px 6px;
    background-color: ${(props) => props.color || ANTD_GRAY[3]};
    :hover {
        opacity: 0.7;
    }
    margin-left: 4px;
    color: ${ANTD_GRAY[8]};
`;

type Props = {
    percentile: number;
    description: React.ReactNode;
};

export const PercentileLabel = ({ percentile, description }: Props) => {
    return (
        <Popover
            color="#262626"
            overlayStyle={{ maxWidth: 260 }}
            placement="bottom"
            showArrow={false}
            content={<Description>{description}</Description>}
        >
            <Label color={percentileToColor(percentile)}>{percentileToLabel(percentile)}</Label>
        </Popover>
    );
};
