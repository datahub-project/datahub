import { Tag } from 'antd';
import styled, { css } from 'styled-components';
import ColorHash from 'color-hash';

const generateColor = new ColorHash({
    saturation: 0.9,
});

export const StyledTag = styled(Tag)<{ $colorHash?: string }>`
    ${(props) =>
        props.$colorHash &&
        css`
            &:before {
                display: inline-block;
                content: '';
                width: 8px;
                height: 8px;
                background: ${generateColor.hex(props.$colorHash)};
                border-radius: 100em;
                margin-right: 3px;
            }
        `}
`;
