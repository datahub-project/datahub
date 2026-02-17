import { ThunderboltFilled } from '@ant-design/icons';
import styled from 'styled-components';


export const PropagateThunderbolt = styled(ThunderboltFilled)`
    && {
        color: #a7c7fa;
    }
    font-size: 16px;
    &:hover {
        color: ${(props) => props.theme.colors.textInformation};
    }
    margin-right: 4px;
`;

export const PropagateThunderboltFilled = styled(ThunderboltFilled)`
    && {
        color: ${(props) => props.theme.colors.textInformation};
    }
    font-size: 16px;
    margin-right: 4px;
`;
