import styled from 'styled-components';
import { Typography } from 'antd';
import { REDESIGN_COLORS } from '../../../../constants';

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

export const InstanceIcon = styled.div`
    height: 22px;
    width: 22px;
    background-color: #c9fff2;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
    svg {
        padding: 3px;
        height: 20px;
        width: 20px;
    }
`;

export const StyledLabel = styled.span`
    font-size: 16px;
    font-weight: 400;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;
