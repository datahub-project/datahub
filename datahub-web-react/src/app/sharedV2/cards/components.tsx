/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

import { ANTD_GRAY, ANTD_GRAY_V2, REDESIGN_COLORS } from '@app/entity/shared/constants';

export const Card = styled.div`
    background-color: ${ANTD_GRAY[1]};
    border: 2px solid ${ANTD_GRAY_V2[5]};
    border-radius: 8px;
    cursor: pointer;
    display: flex;
    align-items: center;

    :hover {
        border: 2px solid ${REDESIGN_COLORS.BLUE};
        cursor: pointer;
    }
`;
