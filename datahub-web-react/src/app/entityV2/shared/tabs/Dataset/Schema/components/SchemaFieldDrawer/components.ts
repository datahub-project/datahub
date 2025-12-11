/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Divider } from 'antd';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { colors } from '@src/alchemy-components';

export const SectionHeader = styled.div`
    margin-bottom: 8px;
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-size: 14px;
    font-style: normal;
    font-weight: 700;
    line-height: 24px;
`;

export const StyledDivider = styled(Divider)`
    border-color: ${colors.gray[100]};
    border-style: solid;
`;
