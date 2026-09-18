import React, { useMemo } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { MFEMount, useSlotPrincipal } from '@app/mfeframework/MFEConfigurableContainer';
import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { EntityDetailTabContext, SLOT_CONTRACT_VERSION } from '@app/mfeframework/slots/slotTypes';

const ENTITY_TAB_MIN_HEIGHT = 320;

type Props = {
    config: MFEConfig;
};

/**
 * Renders one `entity.detail.tab` MFE. Builds the typed context from the entity page the host is
 * already showing; the remote is only loaded when the tab is selected (antd Tabs mounts panes lazily).
 */
export default function MFEEntityTab({ config }: Props) {
    const { urn, entityType } = useEntityData();
    const principal = useSlotPrincipal();

    const ctx = useMemo<EntityDetailTabContext>(
        () => ({
            slot: 'entity.detail.tab',
            version: SLOT_CONTRACT_VERSION,
            entity: { urn, type: entityType },
            ...(principal ? { principal } : {}),
        }),
        [urn, entityType, principal],
    );

    return <MFEMount config={config} ctx={ctx} minHeight={ENTITY_TAB_MIN_HEIGHT} />;
}
