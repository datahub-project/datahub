import { Tag } from 'antd';
import styled, { css } from 'styled-components';
import ColorHash from 'color-hash';

export const generateColor = new ColorHash({
    saturation: 0.9,
});

export const StyledTag = styled(Tag)<{ $color: any; $colorHash?: string; fontSize?: number; highlightTag?: boolean }>`
    &&& {
        ${(props) =>
            props.highlightTag &&
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
