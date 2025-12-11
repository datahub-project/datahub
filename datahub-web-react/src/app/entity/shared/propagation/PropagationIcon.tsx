/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ThunderboltFilled } from '@ant-design/icons';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entity/shared/constants';

export const PropagateThunderbolt = styled(ThunderboltFilled)`
    && {
        color: #a7c7fa;
    }
    font-size: 16px;
    &:hover {
        color: ${REDESIGN_COLORS.BLUE};
    }
    margin-right: 4px;
`;

export const PropagateThunderboltFilled = styled(ThunderboltFilled)`
    && {
        color: ${REDESIGN_COLORS.BLUE};
    }
    font-size: 16px;
    margin-right: 4px;
`;
