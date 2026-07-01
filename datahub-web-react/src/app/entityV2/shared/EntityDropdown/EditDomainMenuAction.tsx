import { Tooltip } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import EditDomainModal from '@app/entityV2/domain/EditDomainModal';
import { ActionMenuItem } from '@app/entityV2/shared/EntityDropdown/styledComponents';

import { EntityType } from '@types';

export default function EditDomainMenuAction() {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { entityType } = useEntityData();
    const me = useUserContext();
    const [isModalVisible, setIsModalVisible] = useState(false);

    if (entityType !== EntityType.Domain) {
        return null;
    }

    const canEdit = !!me.platformPrivileges?.manageDomains;

    return (
        <>
            {/*
             * Force the tooltip closed while the modal is open. Otherwise the tooltip
             * remains visible after click because the cursor never leaves the trigger
             * (the modal opens directly over it), and the modal's overlay does not
             * dispatch a mouseleave. Passing `open={undefined}` re-enables the default
             * hover behavior once the modal closes.
             */}
            <Tooltip
                placement="bottom"
                title={t('menuAction.editDomainTooltip')}
                showArrow={false}
                open={isModalVisible ? false : undefined}
            >
                <ActionMenuItem
                    key="edit"
                    disabled={!canEdit}
                    onClick={() => setIsModalVisible(true)}
                    data-testid="entity-header-edit-domain-button"
                >
                    <PencilSimple size={16} weight="regular" />
                </ActionMenuItem>
            </Tooltip>
            {isModalVisible && <EditDomainModal onClose={() => setIsModalVisible(false)} />}
        </>
    );
}
