/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { formatNumber } from '@app/shared/formatNumber';

const Container = styled.div`
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_4};
    font-size: 12px;
    white-space: nowrap;
`;

interface Props {
    queryCountLast30Days?: number | null;
}

const QueryStat = ({ queryCountLast30Days }: Props) => {
    return <Container>{formatNumber(queryCountLast30Days)} queries</Container>;
};

export default QueryStat;
