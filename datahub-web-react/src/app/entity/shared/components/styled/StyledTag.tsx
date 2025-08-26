import { Tag } from 'antd';
import ColorHash from 'color-hash';
import styled, { css } from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { colors } from '@src/alchemy-components';

export const generateColor = new ColorHash({
    saturation: 0.9,
});

export const StyledTag = styled(Tag)<{
    $color: any;
    $colorHash?: string;
    fontSize?: number;
    $highlightTag?: boolean;
    noMargin?: boolean;
    $isApproved?: boolean;
}>`
    display: inline-flex;
    align-items: center;
    border-radius: 5px !important;
    border: 1px ${(props) => (props.$isApproved ? 'solid' : 'dashed')} ${colors.gray[200]};
    padding: 2px 8px;
    ${(props) => !props.noMargin && `margin-bottom: 7px;`}
    > span {
        color: ${REDESIGN_COLORS.TEXT_GREY};
    }
    &&& {
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
                width: 8px;
                height: 8px;
                background: ${props.$color === null || props.$color === undefined
                    ? generateColor.hex(props.$colorHash)
                    : props.$color};
                border-radius: 100em;
                margin-right: 4px;
            }
        `}
`;
