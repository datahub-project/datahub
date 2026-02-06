import { CloseCircleFilled, ExclamationCircleFilled, InfoCircleFilled, QuestionCircleFilled } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { ERROR_COLOR_HEX, INFO_COLOR_HEX } from '@components/theme/foundations/colors';

import { NO_RESULTS_COLOR_HEX } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { applyOpacityToHexColor } from '@app/shared/styleUtils';

import { AssertionHealth, AssertionHealthStatus, Maybe } from '@types';

const HealthMessageContainer = styled.div<{ $backgroundColor: string }>`
    display: flex;
    gap: 8px;
    align-items: flex-start;
    padding: 8px 12px;
    border-radius: 8px;
    background-color: ${(props) => props.$backgroundColor};
`;

const HealthMessageIcon = styled.div<{ $iconColor: string }>`
    color: ${(props) => props.$iconColor};
    margin-top: 4px;
`;

const HealthMessageText = styled.div<{ $textColor: string }>`
    color: ${(props) => props.$textColor};
    font-size: 14px;
`;

const HealthMessageTitle = styled.div`
    font-weight: 600;
    margin-bottom: 2px;
`;

const HealthMessageDescription = styled.div``;

const HEALTH_MESSAGE_STYLES = {
    [AssertionHealthStatus.Healthy]: {
        backgroundColor: applyOpacityToHexColor(INFO_COLOR_HEX, 0.15),
        textColor: INFO_COLOR_HEX,
        iconColor: INFO_COLOR_HEX,
        icon: <InfoCircleFilled style={{ fontSize: 16 }} />,
    },
    [AssertionHealthStatus.Degraded]: {
        backgroundColor: applyOpacityToHexColor(ERROR_COLOR_HEX, 0.15),
        textColor: ERROR_COLOR_HEX,
        iconColor: ERROR_COLOR_HEX,
        icon: <ExclamationCircleFilled style={{ fontSize: 16 }} />,
    },
    [AssertionHealthStatus.Error]: {
        backgroundColor: applyOpacityToHexColor(ERROR_COLOR_HEX, 0.15),
        textColor: ERROR_COLOR_HEX,
        iconColor: ERROR_COLOR_HEX,
        icon: <CloseCircleFilled style={{ fontSize: 16 }} />,
    },
    [AssertionHealthStatus.Unknown]: {
        backgroundColor: applyOpacityToHexColor(NO_RESULTS_COLOR_HEX, 0.15),
        textColor: NO_RESULTS_COLOR_HEX,
        iconColor: NO_RESULTS_COLOR_HEX,
        icon: <QuestionCircleFilled style={{ fontSize: 16 }} />,
    },
};

type Props = {
    health?: Maybe<AssertionHealth>;
};

export const AssertionHealthMessage = ({ health }: Props) => {
    const healthDisplayMessage = health?.displayMessage;
    const healthRecommendedAction = health?.recommendedAction;
    const healthStatus = health?.status || AssertionHealthStatus.Unknown;
    const showHealthMessage = !!healthDisplayMessage || !!healthRecommendedAction;

    if (!showHealthMessage) {
        return null;
    }

    const healthStyles = HEALTH_MESSAGE_STYLES[healthStatus];
    return (
        <HealthMessageContainer $backgroundColor={healthStyles.backgroundColor}>
            <HealthMessageIcon $iconColor={healthStyles.iconColor}>{healthStyles.icon}</HealthMessageIcon>
            <HealthMessageText $textColor={healthStyles.textColor}>
                {healthDisplayMessage && <HealthMessageTitle>{healthDisplayMessage}</HealthMessageTitle>}
                {healthRecommendedAction && (
                    <HealthMessageDescription>{healthRecommendedAction}</HealthMessageDescription>
                )}
            </HealthMessageText>
        </HealthMessageContainer>
    );
};
