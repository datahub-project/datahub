import { CheckCircle, Circle, CircleDashed, CircleHalf, Hexagon } from '@phosphor-icons/react';
import React, { useMemo } from 'react';
import { useTheme } from 'styled-components';

import { IncidentStageLabel } from '@components/components/IncidentStagePill/constant';
import { Pill } from '@components/components/Pills';

import colors from '@src/alchemy-components/theme/foundations/colors';
import { IncidentStage } from '@src/types.generated';

export const IncidentStagePill = ({ stage, showLabel = false }: { stage: string; showLabel?: boolean }) => {
    const themeInstance = useTheme() as any;
    const tc = themeInstance?.colors;

    const INCIDENT_STAGE = useMemo(
        () => ({
            [IncidentStage.Triage]: {
                bgColor: tc?.bgSurfaceBrand ?? colors.gray[1000],
                color: tc?.iconBrand ?? colors.violet[500],
                icon: <Hexagon size={16} fill={tc?.iconBrand ?? colors.violet[500]} />,
            },
            [IncidentStage.Investigation]: {
                bgColor: tc?.bgSurfaceWarning ?? colors.yellow[1200],
                color: tc?.iconWarning ?? colors.yellow[1000],
                icon: <Circle size={16} fill={tc?.iconWarning ?? colors.yellow[1000]} />,
            },
            [IncidentStage.WorkInProgress]: {
                bgColor: tc?.bgSurfaceInfo ?? colors.gray[1100],
                color: tc?.iconInformation ?? colors.blue[1000],
                icon: <CircleHalf size={16} fill={tc?.iconInformation ?? colors.blue[1000]} />,
            },
            [IncidentStage.Fixed]: {
                bgColor: tc?.bgSurfaceSuccess ?? colors.gray[1300],
                color: tc?.iconSuccess ?? colors.green[1000],
                icon: <CheckCircle size={16} fill={tc?.iconSuccess ?? colors.green[1000]} />,
            },
            [IncidentStage.NoActionRequired]: {
                bgColor: tc?.border ?? colors.gray[100],
                color: tc?.textSecondary ?? colors.gray[1700],
                icon: <CircleDashed size={16} fill={tc?.textSecondary ?? colors.gray[1700]} />,
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
