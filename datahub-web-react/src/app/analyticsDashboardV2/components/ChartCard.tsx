import { Card } from 'antd';
import styled from 'styled-components';

export const ChartCard = styled(Card)<{ $shouldScroll: boolean }>`
    margin: 12px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    height: 440px;
    overflow-y: ${(props) => (props.$shouldScroll ? 'scroll' : 'hidden')};
`;
