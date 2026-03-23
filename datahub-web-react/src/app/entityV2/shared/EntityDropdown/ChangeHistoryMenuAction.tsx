import { Tooltip } from '@components';
import { ClockCounterClockwise } from '@phosphor-icons/react/dist/csr/ClockCounterClockwise';
import React, { useState } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ActionMenuItem } from '@app/entityV2/shared/EntityDropdown/styledComponents';
import HistorySidebar from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar';

export default function ChangeHistoryMenuAction() {
    const { urn, entityType } = useEntityData();
    const [open, setOpen] = useState(false);

    return (
        <>
            <Tooltip placement="bottom" title={open ? 'Close change history' : 'View change history'}>
                <ActionMenuItem key="change-history" onClick={() => setOpen(!open)}>
                    <ClockCounterClockwise size={16} />
                </ActionMenuItem>
            </Tooltip>
            <HistorySidebar
                open={open}
                onClose={() => setOpen(false)}
                urn={urn}
                versionList={[]}
                hideSemanticVersions
                entityType={entityType}
            />
        </>
    );
}
