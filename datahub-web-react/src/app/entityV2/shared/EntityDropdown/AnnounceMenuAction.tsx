import { Tooltip } from '@components';
import { MegaphoneSimple } from '@phosphor-icons/react';
import React, { useState } from 'react';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { ActionMenuItem } from '@app/entityV2/shared/EntityDropdown/styledComponents';
import CreateEntityAnnouncementModal from '@app/entityV2/shared/announce/CreateEntityAnnouncementModal';

export default function AnnounceMenuAction() {
    const { urn } = useEntityData();
    const refetchForEntity = useRefetch();
    const [isEntityAnnouncementModalVisible, setIsEntityAnnouncementModalVisible] = useState(false);

    return (
        <>
            <Tooltip placement="bottom" title="Add Note">
                <ActionMenuItem
                    key="announce"
                    disabled={false}
                    onClick={() => setIsEntityAnnouncementModalVisible(true)}
                >
                    <MegaphoneSimple size={16} />
                </ActionMenuItem>
            </Tooltip>
            {isEntityAnnouncementModalVisible && (
                <CreateEntityAnnouncementModal
                    urn={urn}
                    onClose={() => setIsEntityAnnouncementModalVisible(false)}
                    onCreate={() => setTimeout(() => refetchForEntity?.(), 2000)}
                />
            )}
        </>
    );
}
