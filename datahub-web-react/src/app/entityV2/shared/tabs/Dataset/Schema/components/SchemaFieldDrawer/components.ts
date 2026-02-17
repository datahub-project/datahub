import { Divider } from 'antd';
import styled from 'styled-components';

export const SectionHeader = styled.div`
    margin-bottom: 8px;
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 14px;
    font-style: normal;
    font-weight: 700;
    line-height: 24px;
`;

export const StyledDivider = styled(Divider)`
    border-color: ${(props) => props.theme.colors.border};
    border-style: solid;
`;
