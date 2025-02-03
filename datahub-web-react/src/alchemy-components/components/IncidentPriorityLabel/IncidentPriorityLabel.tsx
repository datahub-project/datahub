import React from 'react';
import { ExclamationMark } from '@phosphor-icons/react';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { IconLabel } from '../IconLabel';
import { IncidentPriorityLabelProps } from './types';
import { Bar } from '../Bar';
import { PRIORITIES } from './constant';
import { IconType } from '../IconLabel/types';

const PRIORITY_LEVEL = {
    [PRIORITIES.HIGH]: 3,
    [PRIORITIES.MEDIUM]: 2,
    [PRIORITIES.LOW]: 1,
};

const renderBars = (priority: string) => {
    return <Bar size="default" color={colors.gray[1800]} coloredBars={PRIORITY_LEVEL[priority]} />;
};

const Icons = {
    [PRIORITIES.CRITICAL]: {
        icon: <ExclamationMark weight="fill" color={colors.gray[1800]} size={22} />,
        type: IconType.ICON,
    },
    [PRIORITIES.HIGH]: {
        icon: renderBars(PRIORITIES.HIGH),
        type: IconType.ICON,
    },
    [PRIORITIES.MEDIUM]: {
        icon: renderBars(PRIORITIES.MEDIUM),
        type: IconType.ICON,
    },
    [PRIORITIES.LOW]: {
        icon: renderBars(PRIORITIES.LOW),
        type: IconType.ICON,
    },
};

export const IncidentPriorityLabel = ({ priority, title, style }: IncidentPriorityLabelProps) => {
    const { icon, type } = Icons[priority] || {};
    return <IconLabel style={style} icon={icon} name={title} type={type} />;
};
