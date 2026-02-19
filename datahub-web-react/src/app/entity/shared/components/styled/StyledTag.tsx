import { Tag } from 'antd';
import ColorHash from 'color-hash';
import styled, { css } from 'styled-components';

export const generateColor = new ColorHash({
    saturation: 0.9,
});

export const StyledTag = styled(Tag)<{ $color: any; $colorHash?: string; fontSize?: number; highlightTag?: boolean }>`
    &&& {
        ${(props) =>
            props.highlightTag &&
            `
                background: ${props.theme.colors.bgHighlight};
                border: 1px solid ${props.theme.colors.borderHover};
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
