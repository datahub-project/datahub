import React from 'react';

import CriticalIcon from '@src/images/incident-critical.svg';
import HighIcon from '@src/images/incident-chart-bar-three.svg';
import MediumIcon from '@src/images/incident-chart-bar-two.svg';
import LowIcon from '@src/images/incident-chart-bar-one.svg';
import { Label, StyledImage } from './components';

import { IconLabel } from '../IconLabel';
import { IncidentPriorityLabelProps } from './types';
import { PRIORITIES } from './constant';
import { IconType } from '../IconLabel/types';

// 🔄 Map priorities to icons for cleaner code
const priorityIcons = {
    [PRIORITIES.CRITICAL]: CriticalIcon,
    [PRIORITIES.HIGH]: HighIcon,
    [PRIORITIES.MEDIUM]: MediumIcon,
    [PRIORITIES.LOW]: LowIcon,
    [PRIORITIES.NONE]: null,
};

// 🚀 Dynamically generate the Icons object
const Icons = Object.fromEntries(
    Object.entries(priorityIcons).map(([priority, iconSrc]) => [
        priority,
        {
            icon: iconSrc ? <StyledImage src={iconSrc} alt={priority} /> : null,
            type: IconType.ICON,
        },
    ]),
);

export const IncidentPriorityLabel = ({ priority, title, style }: IncidentPriorityLabelProps) => {
    const { icon, type } = Icons[priority] || {};
    if (!icon) return <Label>{title}</Label>;
    return <IconLabel style={style} icon={icon} name={title} type={type} />;
};
