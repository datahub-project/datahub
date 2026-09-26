import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useUserContext } from '@app/context/useUserContext';
import CreateLogicalModelModal from '@app/entityV2/shared/logicalModels/CreateLogicalModelModal';
import { useAppConfig } from '@app/useAppConfig';
import { Button, Tooltip } from '@src/alchemy-components';

export default function CreateLogicalModelButton() {
    const { t } = useTranslation('logicalModels');
    const { config } = useAppConfig();
    const { platformPrivileges } = useUserContext();
    const [isOpen, setIsOpen] = useState(false);

    if (!config?.featureFlags?.logicalModelsEnabled || !platformPrivileges?.createLogicalModels) {
        return null;
    }

    return (
        <>
            <Tooltip title={t('modal.title')}>
                <Button
                    isCircle
                    icon={{ icon: Plus }}
                    variant="text"
                    color="gray"
                    size="sm"
                    onClick={() => setIsOpen(true)}
                    data-testid="create-logical-model-trigger"
                />
            </Tooltip>
            {isOpen && <CreateLogicalModelModal onClose={() => setIsOpen(false)} onCreate={() => setIsOpen(false)} />}
        </>
    );
}
