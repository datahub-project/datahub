import { Typography } from 'antd';
import styled from 'styled-components';

import { HeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';

export const MainSection = styled.div`
    display: flex;
    flex-direction: column;
`;

export const SummaryHeader = styled(Typography.Text)`
    margin-bottom: 20px;
    font-size: 18px;
    color: ${(props) => props.theme.colors.text};
    font-weight: 500;
`;

export const VerticalDivider = styled.hr`
    align-self: stretch;
    height: auto;
    margin: 0 20px;
    color: ${(props) => props.theme.colors.border};
    border-width: 1px;
    opacity: 0.2;
`;

export const StyledTitle = styled(HeaderTitle)`
    margin-bottom: 12px;
    font-size: 14px;
    color: ${(props) => props.theme.colors.text};
    font-weight: 700;
`;
