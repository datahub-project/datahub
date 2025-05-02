import React from 'react';

import { IconLabel } from '@components/components/IconLabel';
import { IconType } from '@components/components/IconLabel/types';
import { Label, StyledImage } from '@components/components/IncidentPriorityLabel/components';
import { Priority } from '@components/components/IncidentPriorityLabel/constant';
import { IncidentPriorityLabelProps } from '@components/components/IncidentPriorityLabel/types';

import LowIcon from '@src/images/incident-chart-bar-one.svg';
import HighIcon from '@src/images/incident-chart-bar-three.svg';
import MediumIcon from '@src/images/incident-chart-bar-two.svg';
import CriticalIcon from '@src/images/incident-critical.svg';

// 🔄 Map priorities to icons for cleaner code
const priorityIcons: Record<Priority, string | null> = {
    [Priority.CRITICAL]: CriticalIcon,
    [Priority.HIGH]: HighIcon,
    [Priority.MEDIUM]: MediumIcon,
    [Priority.LOW]: LowIcon,
    [Priority.NONE]: null,
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
) as Record<Priority, { icon: JSX.Element | null; type: IconType }>;

export const IncidentPriorityLabel = ({ priority, title, style }: IncidentPriorityLabelProps) => {
    const { icon, type } = Icons[priority] || {};
    if (!icon) return <Label data-testid="priority-title">{title}</Label>;
    return <IconLabel testId="priority-title" style={style} icon={icon} name={title} type={type} />;
};
