/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import Icon from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { Text, colors } from '@src/alchemy-components';

import NoStatsAvailble from '@images/no-stats-available.svg?react';

const NoDataContainer = styled.div`
    margin: 40px auto;
    display: flex;
    flex-direction: column;
    align-items: center;
`;

const StyledIcon = styled(Icon)`
    font-size: 80px;
    margin-bottom: 6px;
    color: ${colors.white};
`;

export default function NoStats() {
    return (
        <NoDataContainer>
            <StyledIcon component={NoStatsAvailble} />
            <Text color="gray" size="sm">
                No column statistics found
            </Text>
        </NoDataContainer>
    );
}
