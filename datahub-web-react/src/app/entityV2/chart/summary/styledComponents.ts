/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { HeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';

export const MainSection = styled.div`
    display: flex;
    flex-direction: column;
`;

export const SummaryHeader = styled(Typography.Text)`
    margin-bottom: 20px;
    font-size: 18px;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    font-weight: 500;
`;

export const VerticalDivider = styled.hr`
    align-self: stretch;
    height: auto;
    margin: 0 20px;
    color: ${REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1};
    border-width: 1px;
    opacity: 0.2;
`;

export const StyledTitle = styled(HeaderTitle)`
    margin-bottom: 12px;
    font-size: 14px;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    font-weight: 700;
`;
