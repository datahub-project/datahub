import { Divider } from 'antd';
import styled from 'styled-components';

export const StyledDivider = styled(Divider)`
    border-color: ${(props) => props.theme.colors.border};
    border-style: solid;
`;
