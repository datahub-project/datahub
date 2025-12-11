/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entityV2/shared/constants';

export const ControlPanel = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;

    background: #fff;
    border-radius: 8px;
    border: 1px solid ${ANTD_GRAY[5]};
    box-shadow: 0 4px 4px 0 ${REDESIGN_COLORS.BOX_SHADOW};
    padding: 16px;
    gap: 2px;

    height: fit-content;
    max-width: 255px;
    overflow: hidden;
`;

export const ControlPanelTitle = styled.div`
    font-size: 14px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

export const ControlPanelSubtext = styled.div`
    color: ${REDESIGN_COLORS.TEXT_GREY};
    font-size: 10px;
    font-weight: 500;
    margin-bottom: 8px;
`;
