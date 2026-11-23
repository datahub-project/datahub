import React from 'react';

import { IconLabel } from '@components/components/IconLabel';
import { IconType } from '@components/components/IconLabel/types';

import { Label, StyledImage } from '@app/sharedV2/incidentPriorityLabel/components';
import { PRIORITIES } from '@app/sharedV2/incidentPriorityLabel/constant';
import { IncidentPriorityLabelProps } from '@app/sharedV2/incidentPriorityLabel/types';
import { getCapitalizeWord } from '@app/sharedV2/textUtil';
import LowIcon from '@src/images/incident-chart-bar-one.svg';
import HighIcon from '@src/images/incident-chart-bar-three.svg';
import MediumIcon from '@src/images/incident-chart-bar-two.svg';
import CriticalIcon from '@src/images/incident-critical.svg';

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

/** The priority's icon alone, for places that render their own label (e.g. table group headers). */
export const IncidentPriorityIcon = ({ priority }: { priority: string }) => {
    const iconSrc = priorityIcons[priority];
    // 16px to match the phosphor icons it sits next to; the SVGs default to their 22px artboard.
    return iconSrc ? <StyledImage src={iconSrc} alt={getCapitalizeWord(priority)} width={16} height={16} /> : null;
};

export const IncidentPriorityLabel = ({ priority, title: titleProp, style }: IncidentPriorityLabelProps) => {
    const title = titleProp ?? (priority ? getCapitalizeWord(priority) : priority);
    const { icon, type } = Icons[priority] || {};
    if (!icon) return <Label data-testid="priority-title">{title}</Label>;
    return <IconLabel testId="priority-title" style={style} icon={icon} name={title} type={type} />;
};
