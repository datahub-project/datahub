import { Button, Menu } from '@components';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ItemType } from '@components/components/Menu/types';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { DEFAULT_MODULE_URNS } from '@app/homeV3/modules/constants';
import { getCustomGlobalModules } from '@app/homeV3/template/components/addModuleMenu/utils';
import { ModulePositionInput } from '@app/homeV3/template/types';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { PageModuleFragment } from '@graphql/template.generated';

const DropdownWrapper = styled.div`
    display: flex;
    align-items: center;
`;

interface Props {
    module: PageModuleFragment;
    position: ModulePositionInput;
}

export default function ModuleMenu({ module, position }: Props) {
    const { t } = useTranslation('modules');
    const { t: tc } = useTranslation('common.actions');
    const [showRemoveModuleConfirmation, setShowRemoveModuleConfirmation] = useState<boolean>(false);
    const { type } = module.properties;
    const canEdit = !DEFAULT_MODULE_URNS.includes(module.urn);

    const { globalTemplate } = usePageTemplateContext();
    const isAdminCreatedModule = useMemo(() => {
        const adminCreatedModules = getCustomGlobalModules(globalTemplate);
        return adminCreatedModules.some((adminCreatedModule) => adminCreatedModule.urn === module.urn);
    }, [globalTemplate, module.urn]);

    const {
        removeModule,
        moduleModalState: { openToEdit },
    } = usePageTemplateContext();

    const handleEditModule = useCallback(() => {
        openToEdit(type, module, position);
    }, [module, openToEdit, type, position]);

    const handleRemove = useCallback(() => {
        removeModule({
            module,
            position,
        });
        setShowRemoveModuleConfirmation(false);
    }, [removeModule, module, position]);

    const handleMenuClick = useCallback((e: React.MouseEvent) => {
        e.stopPropagation();
    }, []);

    const items: ItemType[] = useMemo(
        () => [
            {
                type: 'item',
                key: 'edit',
                title: tc('edit'),
                disabled: !canEdit,
                tooltip: canEdit ? undefined : t('menu.defaultModulesNotEditable'),
                onClick: handleEditModule,
                dataTestId: 'edit-module',
            },
            {
                type: 'item',
                key: 'remove',
                title: tc('remove'),
                danger: true,
                onClick: () => setShowRemoveModuleConfirmation(true),
                dataTestId: 'remove-module',
            },
        ],
        [t, tc, canEdit, handleEditModule],
    );

    return (
        <>
            <DropdownWrapper onClick={handleMenuClick}>
                <Menu items={items} trigger={['click']}>
                    <Button
                        variant="text"
                        icon={{ icon: DotsThreeVertical, weight: 'bold', size: 'xl', color: 'icon' }}
                        isCircle
                        data-testid="module-options"
                    />
                </Menu>
            </DropdownWrapper>

            <ConfirmationModal
                isOpen={!!showRemoveModuleConfirmation}
                handleConfirm={handleRemove}
                handleClose={() => setShowRemoveModuleConfirmation(false)}
                modalTitle={t('menu.removeModuleTitle')}
                modalText={isAdminCreatedModule ? t('menu.removeAdminModuleText') : t('menu.removeModuleText')}
                closeButtonText={tc('cancel')}
                confirmButtonText={tc('remove')}
                isDeleteModal
            />
        </>
    );
}
