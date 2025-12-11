/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button } from 'antd';
import styled from 'styled-components';

import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entityV2/shared/constants';

const MenuItem = styled.div`
    font-size: 12px;
    padding: 0 4px;
    color: #262626;
`;

export const ActionMenuItem = styled(Button)<{ disabled?: boolean; fontSize?: number }>`
    flex-shrink: 0;
    border-radius: 20px;
    width: ${(props) => (props.fontSize ? `${props.fontSize}px` : '28px')};
    height: ${(props) => (props.fontSize ? `${props.fontSize}px` : '28px')};
    padding: 0px;
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    border: none;
    background-color: 'white';
    border: 1px solid #eee;
    color: ${REDESIGN_COLORS.ACTION_ICON_GREY};
    box-shadow: none;
    &&:hover {
        background-color: ${ANTD_GRAY[3]};
        color: ${(props) => props.theme.styles['primary-color']};
        border-color: ${(props) => props.theme.styles['primary-color']};
    }
    ${(props) =>
        props.disabled
            ? `
            ${MenuItem} {
                color: ${ANTD_GRAY[7]};
            }
    `
            : ''};
`;
