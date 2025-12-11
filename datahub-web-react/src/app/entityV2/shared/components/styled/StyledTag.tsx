/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { colors } from '@components';
import { Tag } from 'antd';
import ColorHash from 'color-hash';
import styled, { css } from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

export const generateColor = new ColorHash({
    saturation: 0.9,
});

export const StyledTag = styled(Tag)<{
    $color: any;
    $colorHash?: string;
    fontSize?: number;
    $highlightTag?: boolean;
    $showOneAndCount?: boolean;
}>`
    display: flex;
    align-items: center;
    margin: 0;
    max-width: 200px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    &&& {
        border-color: ${colors.gray[100]};
        ${(props) =>
            props.$highlightTag &&
            `
                background: ${props.theme.styles['highlight-color']};
                border: 1px solid ${props.theme.styles['highlight-border-color']};
            `}
    }
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
    ${(props) =>
        props.$colorHash &&
        css`
            &:before {
                display: inline-block;
                content: '';
                min-width: 8px;
                min-height: 8px;
                background: ${props.$color === null || props.$color === undefined
                    ? generateColor.hex(props.$colorHash)
                    : props.$color};
                border-radius: 100em;
                margin-right: 4px;
            }
        `};
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-weight: 400;
    ${(props) =>
        props.$showOneAndCount &&
        `
            width: 100%;
            max-width: max-content;
            overflow: hidden;
            text-overflow: ellipsis;
            vertical-align: middle;
        `}
`;
