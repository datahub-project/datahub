import React, { useMemo } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { MFEMount, useSlotPrincipal } from '@app/mfeframework/MFEConfigurableContainer';
import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { buildSlotContext } from '@app/mfeframework/slots/slotContextBuilders';

const ENTITY_TAB_MIN_HEIGHT = 320;

type Props = {
    config: MFEConfig;
};

/**
 * Renders one `entity.detail.tab` MFE. The context is built by the builder registered for the entry's
 * declared `placement.contractVersion`, from the entity page the host is already showing; the remote is
 * only loaded when the tab is selected (antd Tabs mounts panes lazily).
 */
export default function MFEEntityTab({ config }: Props) {
    const { urn, entityType } = useEntityData();
    const principal = useSlotPrincipal();

    const ctx = useMemo(
        () => buildSlotContext(config, { urn, entityType, principal }),
        [config, urn, entityType, principal],
    );

    // Gate 2 — unsupported contract version. buildSlotContext already logged; render nothing rather
    // than hand the remote a shape it was not built for. resolveSlot normally drops these before we
    // get here, so this is defence in depth for any direct render.
    if (!ctx) return null;

    return <MFEMount config={config} ctx={ctx} minHeight={ENTITY_TAB_MIN_HEIGHT} />;
}
