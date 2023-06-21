import { Button } from 'antd';
import styled from 'styled-components';

export const TextButton = styled(Button)<{ marginTop?: number; height?: number }>`
    color: ${(props) => props.theme.styles['primary-color']};
    padding: 0px 6px;
    margin-top: ${(props) => (props.marginTop !== undefined ? `${props.marginTop}px` : '8px')};
    ${(props) => props.height !== undefined && `height: ${props.height}px;`}

    &:hover {
        background-color: white;
    }
`;
