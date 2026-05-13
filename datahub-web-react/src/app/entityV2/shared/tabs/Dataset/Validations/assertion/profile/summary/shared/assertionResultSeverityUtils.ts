import React from 'react';

import LowSeverityIcon from '@src/images/incident-chart-bar-one.svg?react';
import HighSeverityIcon from '@src/images/incident-chart-bar-three.svg?react';
import MediumSeverityIcon from '@src/images/incident-chart-bar-two.svg?react';

import { AssertionResult, AssertionResultType } from '@types';

export type SeverityDisplay = {
    label: string;
    icon: React.ComponentType<React.SVGProps<SVGSVGElement>>;
};

export const getAssertionResultSeverityDisplay = (result?: AssertionResult): SeverityDisplay | undefined => {
    if (result?.type !== AssertionResultType.Failure || !result.severity) return undefined;

    switch (String(result.severity).toUpperCase()) {
        case 'HIGH':
            return { label: 'High severity', icon: HighSeverityIcon };
        case 'MEDIUM':
            return { label: 'Medium severity', icon: MediumSeverityIcon };
        case 'LOW':
            return { label: 'Low severity', icon: LowSeverityIcon };
        default:
            return undefined;
    }
};
