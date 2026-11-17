import { CheckCircle } from '@phosphor-icons/react/dist/csr/CheckCircle';
import { Circle } from '@phosphor-icons/react/dist/csr/Circle';
import { CircleDashed } from '@phosphor-icons/react/dist/csr/CircleDashed';
import { CircleHalf } from '@phosphor-icons/react/dist/csr/CircleHalf';
import { Hexagon } from '@phosphor-icons/react/dist/csr/Hexagon';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';

import { Pill } from '@components/components/Pills';

import { getIncidentStageLabel } from '@app/sharedV2/incidentStagePill/constant';
import { IncidentStage } from '@src/types.generated';

const useIncidentStageConfig = () => {
    const theme = useTheme();
    return useMemo(
        () => ({
            [IncidentStage.Triage]: {
                bgColor: theme.colors.bgSurfaceBrand,
                color: theme.colors.textBrand,
                icon: <Hexagon size={16} fill={theme.colors.iconBrand} />,
            },
            [IncidentStage.Investigation]: {
                bgColor: theme.colors.bgSurfaceWarning,
                color: theme.colors.textWarning,
                icon: <Circle size={16} fill={theme.colors.iconWarning} />,
            },
            [IncidentStage.WorkInProgress]: {
                bgColor: theme.colors.bgSurfaceInfo,
                color: theme.colors.textInformation,
                icon: <CircleHalf size={16} fill={theme.colors.iconInformation} />,
            },
            [IncidentStage.Fixed]: {
                bgColor: theme.colors.bgSurfaceSuccess,
                color: theme.colors.textSuccess,
                icon: <CheckCircle size={16} fill={theme.colors.iconSuccess} />,
            },
            [IncidentStage.NoActionRequired]: {
                bgColor: theme.colors.bgSurface,
                color: theme.colors.textSecondary,
                icon: <CircleDashed size={16} fill={theme.colors.icon} />,
            },
        }),
        [theme],
    );
};

/** The stage's icon alone, for places that render their own label (e.g. table group headers). */
export const IncidentStageIcon = ({ stage }: { stage: string }) => {
    const stageConfig = useIncidentStageConfig();
    return stageConfig[stage]?.icon ?? null;
};

export const IncidentStagePill = ({ stage, showLabel = false }: { stage: string; showLabel?: boolean }) => {
    const { t } = useTranslation('entity.profile.incident');
    const { t: tc } = useTranslation('common.labels');
    const stageConfig = useIncidentStageConfig();

    if (!stage) return <Pill label={tc('none')} size="md" />;

    const { icon, color, bgColor } = stageConfig[stage] || {};

    function iconRenderer() {
        return icon;
    }

    const stageLabels = getIncidentStageLabel(t);
    const stageLabel = stageLabels[stage] ?? tc('none');

    return (
        <div title={stageLabel}>
            <Pill
                label={stageLabel}
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
