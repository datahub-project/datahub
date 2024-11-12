import { Tag } from 'antd';
import styled, { css } from 'styled-components';
import ColorHash from 'color-hash';
import { REDESIGN_COLORS } from '../../constants';

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
