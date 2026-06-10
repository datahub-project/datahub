import { Tooltip } from '@components';
import { ClockCounterClockwise } from '@phosphor-icons/react/dist/csr/ClockCounterClockwise';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ActionMenuItem } from '@app/entityV2/shared/EntityDropdown/styledComponents';
import HistorySidebar from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar';

export default function ChangeHistoryMenuAction() {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { urn, entityType } = useEntityData();
    const [open, setOpen] = useState(false);

    return (
        <>
            <Tooltip placement="bottom" title={open ? t('changeHistory.closeTooltip') : t('changeHistory.viewTooltip')}>
                <ActionMenuItem key="change-history" onClick={() => setOpen(!open)}>
                    <ClockCounterClockwise size={16} />
                </ActionMenuItem>
            </Tooltip>
            {open && (
                <HistorySidebar
                    open
                    onClose={() => setOpen(false)}
                    urn={urn}
                    versionList={[]}
                    hideSemanticVersions
                    entityType={entityType}
                />
            )}
        </>
    );
}
