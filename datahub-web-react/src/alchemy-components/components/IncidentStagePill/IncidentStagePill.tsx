import { CheckCircle, Circle, CircleDashed, CircleHalf, Hexagon } from '@phosphor-icons/react';
import React, { useMemo } from 'react';
import { useTheme } from 'styled-components';

import { IncidentStageLabel } from '@components/components/IncidentStagePill/constant';
import { Pill } from '@components/components/Pills';

import { IncidentStage } from '@src/types.generated';

export const IncidentStagePill = ({ stage, showLabel = false }: { stage: string; showLabel?: boolean }) => {
    const theme = useTheme();
    const tc = theme?.colors;

    const INCIDENT_STAGE = useMemo(
        () => ({
            [IncidentStage.Triage]: {
                bgColor: tc?.bgSurfaceBrand,
                color: tc?.iconBrand,
                icon: <Hexagon size={16} fill={tc?.iconBrand} />,
            },
            [IncidentStage.Investigation]: {
                bgColor: tc?.bgSurfaceWarning,
                color: tc?.iconWarning,
                icon: <Circle size={16} fill={tc?.iconWarning} />,
            },
            [IncidentStage.WorkInProgress]: {
                bgColor: tc?.bgSurfaceInfo,
                color: tc?.iconInformation,
                icon: <CircleHalf size={16} fill={tc?.iconInformation} />,
            },
            [IncidentStage.Fixed]: {
                bgColor: tc?.bgSurfaceSuccess,
                color: tc?.iconSuccess,
                icon: <CheckCircle size={16} fill={tc?.iconSuccess} />,
            },
            [IncidentStage.NoActionRequired]: {
                bgColor: tc?.border,
                color: tc?.textSecondary,
                icon: <CircleDashed size={16} fill={tc?.textSecondary} />,
            },
        }),
        [tc],
    );

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
