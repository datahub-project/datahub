import { Divider } from 'antd';
import styled from 'styled-components';

export const StyledDivider = styled(Divider)`
    margin: 0;
    color: ${(props) => props.theme.colors.border};
`;
