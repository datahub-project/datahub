import { Robot } from '@phosphor-icons/react/dist/csr/Robot';
import React from 'react';
import styled from 'styled-components';

import { type EnabledToolItem } from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';
import { Pill, Tooltip } from '@src/alchemy-components';

// Tool names are identifiers, so render them monospaced.
const MONO_STYLE = { fontFamily: 'monospace' } as const;

// Longer tool and agent names are cut so one pill can't widen the row. The full
// name is shown on hover.
const MAX_LABEL_CHARS = 20;

// antd Tooltip needs a box to bind hover to; a bare Pill doesn't reliably trigger it.
const TooltipTarget = styled.span`
    display: inline-flex;
`;

type ToolItemPillProps = {
    item: EnabledToolItem;
};

/** One enabled tool or agent, rendered as a monospace pill; agents carry a robot icon. */
export function ToolItemPill({ item }: ToolItemPillProps) {
    const isTruncated = item.label.length > MAX_LABEL_CHARS;
    const label = isTruncated ? `${item.label.slice(0, MAX_LABEL_CHARS)}…` : item.label;
    const pill = (
        <Pill
            label={label}
            color="primary"
            size="sm"
            leftIcon={item.agentUrn ? Robot : undefined}
            customStyle={MONO_STYLE}
        />
    );
    // A cut name leads the tooltip so the full name is still readable.
    const tooltip = [isTruncated ? item.label : undefined, item.description].filter(Boolean).join(' — ');
    if (!tooltip) return pill;
    return (
        <Tooltip title={tooltip} showArrow={false}>
            <TooltipTarget>{pill}</TooltipTarget>
        </Tooltip>
    );
}
