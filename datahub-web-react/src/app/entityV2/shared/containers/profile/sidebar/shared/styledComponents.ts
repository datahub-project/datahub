import styled from 'styled-components';
import { Typography } from 'antd';
import { REDESIGN_COLORS } from '../../../../constants';

export const RelativeTime = styled.div<{ isRecentlyUpdated?: boolean }>`
    display: flex;
    padding: 3px 8px;
    border-radius: 20px;
    background-color: ${(props) =>
        props.isRecentlyUpdated ? `${REDESIGN_COLORS.GREEN_LIGHT}` : `${REDESIGN_COLORS.RED_LIGHT}`};
    color: ${(props) =>
        props.isRecentlyUpdated ? `${REDESIGN_COLORS.GREEN_NORMAL}` : `${REDESIGN_COLORS.RED_NORMAL}`};
`;

export const ContentText = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
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
