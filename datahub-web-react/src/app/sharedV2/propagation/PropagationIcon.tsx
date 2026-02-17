import { ThunderboltFilled } from '@ant-design/icons';
import styled from 'styled-components';


export const PropagateThunderbolt = styled(ThunderboltFilled)<{ fontSize?: number }>`
    && {
        color: #a7c7fa;
    }
    font-size: ${(props) => props.fontSize || 16}px;
    &:hover {
        color: ${(props) => props.theme.colors.textInformation};
    }
    margin-right: 4px;
`;

export const PropagateThunderboltFilled = styled(ThunderboltFilled)<{ fontSize?: number }>`
    && {
        color: ${(props) => props.theme.colors.textInformation};
    }
    font-size: ${(props) => props.fontSize || 16}px;
    margin-right: 4px;
`;
