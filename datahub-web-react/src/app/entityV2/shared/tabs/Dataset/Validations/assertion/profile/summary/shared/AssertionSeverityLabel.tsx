import React from 'react';
import styled, { DefaultTheme, useTheme } from 'styled-components';

import LowSeverityIcon from '@src/images/incident-chart-bar-one.svg?react';
import HighSeverityIcon from '@src/images/incident-chart-bar-three.svg?react';
import MediumSeverityIcon from '@src/images/incident-chart-bar-two.svg?react';

import { AssertionResult, AssertionResultType } from '@types';

const Label = styled.span`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    white-space: nowrap;
`;

const Icon = styled.svg<{ $color: string }>`
    width: 16px;
    height: 16px;
    color: ${(props) => props.$color};

    path {
        fill: currentColor;
    }
`;

type SeverityDisplay = {
    label: string;
    color: string;
    icon: React.ComponentType<React.SVGProps<SVGSVGElement>>;
};

type Props = {
    result?: AssertionResult;
};

const getSeverityDisplay = (severity: string | undefined, theme: DefaultTheme): SeverityDisplay | undefined => {
    switch (severity?.toUpperCase()) {
        case 'HIGH':
            return {
                label: 'High severity',
                color: theme.colors.iconError,
                icon: HighSeverityIcon,
            };
        case 'MEDIUM':
            return {
                label: 'Medium severity',
                color: theme.colors.iconWarning,
                icon: MediumSeverityIcon,
            };
        case 'LOW':
            return {
                label: 'Low severity',
                color: theme.colors.iconInformation,
                icon: LowSeverityIcon,
            };
        default:
            return undefined;
    }
};

export const AssertionSeverityLabel = ({ result }: Props) => {
    const theme = useTheme();

    if (result?.type !== AssertionResultType.Failure || !result.severity) {
        return null;
    }

    const display = getSeverityDisplay(String(result.severity), theme);
    if (!display) {
        return null;
    }

    return (
        <Label aria-label={display.label} title={display.label}>
            <Icon as={display.icon} $color={display.color} aria-hidden />
        </Label>
    );
};
