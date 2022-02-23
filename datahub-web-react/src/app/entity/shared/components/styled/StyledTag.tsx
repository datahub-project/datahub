import { Tag } from 'antd';
import styled, { css } from 'styled-components';

export const StyledTag = styled(Tag)<{ $colorHash?: string | null }>`
    ${(props) =>
        props.$colorHash &&
        css`
            &:before {
                display: inline-block;
                content: '';
                width: 8px;
                height: 8px;
                background: ${props.$colorHash};
                border-radius: 100em;
                margin-right: 3px;
            }
        `}
`;
