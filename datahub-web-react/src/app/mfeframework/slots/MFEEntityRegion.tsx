import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { MFEMount, useSlotPrincipal } from '@app/mfeframework/MFEConfigurableContainer';
import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { buildSlotContext } from '@app/mfeframework/slots/slotContextBuilders';
import { EntityRegionSlotId } from '@app/mfeframework/slots/slotTypes';
import { isSlotTabVisible } from '@app/mfeframework/slots/slotVisibility';
import { useResolveSlot } from '@app/mfeframework/slots/useResolveSlot';

// Small reserve so the page does not jump much while the remote loads; the remote sets its own height.
const REGION_MIN_HEIGHT = 48;

const Region = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

export function RegionEntry({ config }: { config: MFEConfig }) {
    const { urn, entityType } = useEntityData();
    const principal = useSlotPrincipal();
    const ctx = React.useMemo(
        () => buildSlotContext(config, { urn, entityType, principal }),
        [config, urn, entityType, principal],
    );
    if (!ctx) return null;
    return <MFEMount config={config} ctx={ctx} minHeight={REGION_MIN_HEIGHT} />;
}

/**
 * Renders every MFE placed in an inline entity-page region, in config order. Uses the same two
 * visibility layers as slot tabs: `entityTypes` in resolveSlot, then `visibleWhen` against the entity on
 * screen, both before any remote loads. Renders nothing when no entry applies, so the host layout is
 * unchanged for deployments that place nothing here.
 */
export default function MFEEntityRegion({ slot }: { slot: EntityRegionSlotId }) {
    const { entityType, entityData } = useEntityData();
    const configs = useResolveSlot(slot, { entityType });
    const visible = configs.filter((config) => isSlotTabVisible(config, entityType, entityData));
    if (visible.length === 0) return null;
    return (
        <Region data-testid={`mfe-region-${slot}`}>
            {visible.map((config) => (
                <RegionEntry key={config.id} config={config} />
            ))}
        </Region>
    );
}
