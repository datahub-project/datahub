import React from 'react';
import styled, { useTheme } from 'styled-components';

import { Pill, Text, Tooltip } from '@src/alchemy-components';
import { ColorOptions, SizeOptions } from '@src/alchemy-components/theme/config/types';

const StyledTooltip = styled(Tooltip)`
    .ant-tooltip-inner {
        border-radius: 8px;
    }
`;

const BadgeContainer = styled.div`
    display: flex;
`;

type LifecycleStageTypeProps = {
    urn: string;
    name: string;
    description?: string | null;
};

type Props = {
    lifecycleStage?: LifecycleStageTypeProps | null;
    size?: SizeOptions;
};

/** Extract the stage ID from a lifecycleStageType URN, e.g. "urn:li:lifecycleStageType:IN_REVIEW" → "IN_REVIEW". */
function getStageId(urn: string): string {
    return urn.split(':').pop() ?? urn;
}

/** Convert a stage ID to a human-readable label, e.g. "IN_REVIEW" → "In Review". */
function getStageLabel(stageId: string): string {
    return stageId
        .split('_')
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
        .join(' ');
}

const STAGE_COLORS: Record<string, ColorOptions> = {
    DRAFT: 'gray',
    UNCONFIGURED: 'gray',
    IN_REVIEW: 'yellow',
    REJECTED: 'red',
    PUBLISHED: 'green',
    DEPRECATED: 'yellow',
    ARCHIVED: 'gray',
};

function getStageColor(stageId: string): ColorOptions {
    return STAGE_COLORS[stageId] ?? 'primary';
}

/**
 * Displays the entity's lifecycle stage (DRAFT, IN_REVIEW, PUBLISHED, etc.) as a Pill badge
 * next to the entity name in the profile header.
 *
 * Renders nothing when lifecycleStage is null — the entity is in the default active state.
 * Takes priority over the StructuredPropertyBadge (the two are mutually exclusive).
 */
export default function LifecycleStageBadge({ lifecycleStage, size = 'md' }: Props) {
    const theme = useTheme();

    if (!lifecycleStage) return null;

    const stageId = getStageId(lifecycleStage.urn);
    const label = lifecycleStage.name ?? getStageLabel(stageId);
    const color = getStageColor(stageId);
    const { description } = lifecycleStage;

    const tooltipContent = description ? (
        <Text color="gray" size="sm">
            {description}
        </Text>
    ) : null;

    return (
        <StyledTooltip
            showArrow={false}
            title={tooltipContent}
            color={theme.colors.bg}
            overlayInnerStyle={{ padding: 12, maxWidth: 260 }}
        >
            <BadgeContainer>
                <Pill label={label} size={size} color={color} clickable={false} />
            </BadgeContainer>
        </StyledTooltip>
    );
}
