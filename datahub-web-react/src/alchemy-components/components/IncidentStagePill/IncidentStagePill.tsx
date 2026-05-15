import { CheckCircle } from '@phosphor-icons/react/dist/csr/CheckCircle';
import { Circle } from '@phosphor-icons/react/dist/csr/Circle';
import { CircleDashed } from '@phosphor-icons/react/dist/csr/CircleDashed';
import { CircleHalf } from '@phosphor-icons/react/dist/csr/CircleHalf';
import { Hexagon } from '@phosphor-icons/react/dist/csr/Hexagon';
import React, { useMemo } from 'react';
import { useTheme } from 'styled-components';

import { IncidentStageLabel } from '@components/components/IncidentStagePill/constant';
import { Pill } from '@components/components/Pills';

import { IncidentStage } from '@src/types.generated';

export const IncidentStagePill = ({ stage, showLabel = false }: { stage: string; showLabel?: boolean }) => {
    const theme = useTheme();

    const stageConfig = useMemo(
        () => ({
            [IncidentStage.Triage]: {
                bgColor: theme.colors.bgSurfaceBrand,
                color: theme.colors.textBrand,
                icon: <Hexagon size={16} fill={theme.colors.iconBrand} />,
            },
            [IncidentStage.Investigation]: {
                bgColor: theme.colors.bgSurfaceWarning,
                color: theme.colors.textWarning,
                icon: <Circle size={16} fill={theme.colors.textWarning} />,
            },
            [IncidentStage.WorkInProgress]: {
                bgColor: theme.colors.bgSurfaceInfo,
                color: theme.colors.textInformation,
                icon: <CircleHalf size={16} fill={theme.colors.textInformation} />,
            },
            [IncidentStage.Fixed]: {
                bgColor: theme.colors.bgSurfaceSuccess,
                color: theme.colors.textSuccess,
                icon: <CheckCircle size={16} fill={theme.colors.textSuccess} />,
            },
            [IncidentStage.NoActionRequired]: {
                bgColor: theme.colors.bgSurface,
                color: theme.colors.textSecondary,
                icon: <CircleDashed size={16} fill={theme.colors.textSecondary} />,
            },
        }),
        [theme],
    );

    if (!stage) return <Pill label="None" size="md" />;

    const { icon, color, bgColor } = stageConfig[stage] || {};

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
