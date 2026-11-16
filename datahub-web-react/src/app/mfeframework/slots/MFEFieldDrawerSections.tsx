import React from 'react';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { MFEMount, useSlotPrincipal } from '@app/mfeframework/MFEConfigurableContainer';
import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { buildSlotContext } from '@app/mfeframework/slots/slotContextBuilders';
import { isSlotTabVisible } from '@app/mfeframework/slots/slotVisibility';
import { useResolveSlot } from '@app/mfeframework/slots/useResolveSlot';

function FieldDrawerEntry({ config, fieldPath }: { config: MFEConfig; fieldPath: string }) {
    const { urn, entityType } = useEntityData();
    const principal = useSlotPrincipal();
    const ctx = React.useMemo(
        () => buildSlotContext(config, { urn, entityType, principal, fieldPath }),
        [config, urn, entityType, principal, fieldPath],
    );
    if (!ctx) return null;
    return <MFEMount config={config} ctx={ctx} minHeight={48} />;
}

/**
 * MFEs placed in `schemaField.drawer.section`, rendered in config order inside the column drawer's About
 * panel. Same visibility layers as the other entity slots, evaluated against the dataset on screen.
 */
export default function MFEFieldDrawerSections({ fieldPath }: { fieldPath: string }) {
    const { entityType, entityData } = useEntityData();
    const configs = useResolveSlot('schemaField.drawer.section', { entityType });
    const visible = configs.filter((config) => isSlotTabVisible(config, entityType, entityData));
    if (!visible.length) return null;
    return (
        <>
            {visible.map((config) => (
                <FieldDrawerEntry key={config.id} config={config} fieldPath={fieldPath} />
            ))}
        </>
    );
}
