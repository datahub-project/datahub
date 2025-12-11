/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ViewOptionTooltipTitle } from '@app/entity/view/select/ViewOptionTooltipTitle';

const ViewName = styled.span`
    width: 200px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

type Props = {
    name: string;
    description?: string | null;
};

export const ViewOptionName = ({ name, description }: Props) => {
    return (
        <Tooltip placement="bottom" showArrow title={<ViewOptionTooltipTitle name={name} description={description} />}>
            <ViewName>{name}</ViewName>
        </Tooltip>
    );
};
