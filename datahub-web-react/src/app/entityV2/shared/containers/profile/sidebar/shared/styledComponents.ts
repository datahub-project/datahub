import { Typography } from 'antd';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

export const RelativeTime = styled.div<{ relativeTimeColor: string }>`
    display: flex;
    padding: 2px 8px;
    border-radius: 20px;
    border: 1px solid;
    border-color: ${(props) => props.relativeTimeColor};
    color: ${(props) => props.relativeTimeColor};
`;

export const ContentText = styled(Typography.Text)<{ color?: string }>`
    font-size: 12px;
    font-weight: 500;
    color: ${(props) => props.color || REDESIGN_COLORS.TEXT_HEADING};
`;

export const LabelText = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;
