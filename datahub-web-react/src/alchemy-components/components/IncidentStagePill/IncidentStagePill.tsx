import React from 'react';
import { Hexagon, Circle, CircleHalf, CheckCircle, CircleDashed } from '@phosphor-icons/react';
import { IncidentStage } from '@src/types.generated';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { Pill } from '../Pills';
import { IncidentStageLabel } from './constant';

const INCIDENT_STAGE = {
    [IncidentStage.Triage]: {
        bgColor: colors.gray[1000],
        color: colors.violet[500],
        icon: <Hexagon size={16} fill={colors.violet[500]} />,
    },
    [IncidentStage.Investigation]: {
        bgColor: colors.yellow[1200],
        color: colors.yellow[1000],
        icon: <Circle size={16} fill={colors.yellow[1000]} />,
    },
    [IncidentStage.WorkInProgress]: {
        bgColor: colors.gray[1100],
        color: colors.blue[1000],
        icon: <CircleHalf size={16} fill={colors.blue[1000]} />,
    },
    [IncidentStage.Fixed]: {
        bgColor: colors.gray[1300],
        color: colors.green[1000],
        icon: <CheckCircle size={16} fill={colors.green[1000]} />,
    },
    [IncidentStage.NoActionRequired]: {
        bgColor: colors.gray[100],
        color: colors.gray[1700],
        icon: <CircleDashed size={16} fill={colors.gray[1700]} />,
    },
};

export const IncidentStagePill = ({ stage, showLabel = false }: { stage: string; showLabel?: boolean }) => {
    if (!stage) return <Pill label="None" size="md" />;

    const { icon, color, bgColor } = INCIDENT_STAGE[stage] || {};

    function iconRenderer() {
        return icon;
    }

    return (
        <div title={IncidentStageLabel[stage] || 'None'}>
            <Pill
                label={IncidentStageLabel[stage] || 'None'}
                size="md"
                customIconRenderer={iconRenderer}
                customStyle={{
                    backgroundColor: bgColor,
                    color,
                }}
                showLabel={showLabel}
            />
        </div>
    );
};
