import { Icon, Text, Tooltip } from '@components';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { Dropdown } from 'antd';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { DEFAULT_MODULE_URNS } from '@app/homeV3/modules/constants';
import { getCustomGlobalModules } from '@app/homeV3/template/components/addModuleMenu/utils';
import { ModulePositionInput } from '@app/homeV3/template/types';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { PageModuleFragment } from '@graphql/template.generated';

const StyledIcon = styled(Icon)`
    height: 100%;
    :hover {
        cursor: pointer;
    }
` as typeof Icon;

const DropdownWrapper = styled.div`
    height: 100%;
`;

const StyledDropdownContainer = styled.div`
    .ant-dropdown-menu {
        border-radius: 12px;
    }
`;

const DisabledText = styled(Text)`
    color: ${(props) => props.theme.colors.textDisabled};
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

    const menuItemStyle = { fontSize: '14px', padding: '5px 16px' };

    const menu = {
        items: [
            {
                title: tc('edit'),
                key: 'edit',
                label: (
                    <>
                        {!canEdit ? (
                            <Tooltip title={t('menu.defaultModulesNotEditable')}>
                                <DisabledText>{tc('edit')}</DisabledText>
                            </Tooltip>
                        ) : (
                            <Text>{tc('edit')}</Text>
                        )}
                    </>
                ),
                style: {
                    ...menuItemStyle,
                },
                onClick: handleEditModule,
                disabled: !canEdit,
                'data-testid': 'edit-module',
            },

            {
                title: tc('remove'),
                label: tc('remove'),
                key: 'remove',
                danger: true,
                style: {
                    ...menuItemStyle,
                },
                onClick: () => setShowRemoveModuleConfirmation(true),
                'data-testid': 'remove-module',
            },
        ],
    };

    return (
        <>
            <DropdownWrapper onClick={handleMenuClick} data-testid="module-options">
                <Dropdown
                    trigger={['click']}
                    dropdownRender={(originNode) => <StyledDropdownContainer>{originNode}</StyledDropdownContainer>}
                    menu={menu}
                >
                    <StyledIcon icon={DotsThreeVertical} size="lg" />
                </Dropdown>
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
