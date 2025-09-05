import { Icon, Text, Tooltip, colors } from '@components';
import { Dropdown } from 'antd';
import React, { useCallback, useMemo, useState } from 'react';
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

interface Props {
    module: PageModuleFragment;
    position: ModulePositionInput;
}

export default function ModuleMenu({ module, position }: Props) {
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
                title: 'Edit',
                key: 'edit',
                label: (
                    <>
                        {!canEdit ? (
                            <Tooltip title="Default modules are not editable">
                                <Text color="gray" colorLevel={300}>
                                    Edit
                                </Text>
                            </Tooltip>
                        ) : (
                            <Text color="gray" colorLevel={600}>
                                Edit
                            </Text>
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
                title: 'Remove',
                label: 'Remove',
                key: 'remove',
                style: {
                    ...menuItemStyle,
                    color: colors.red[500],
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
                    <StyledIcon icon="DotsThreeVertical" source="phosphor" size="lg" />
                </Dropdown>
            </DropdownWrapper>

            <ConfirmationModal
                isOpen={!!showRemoveModuleConfirmation}
                handleConfirm={handleRemove}
                handleClose={() => setShowRemoveModuleConfirmation(false)}
                modalTitle="Remove Module?"
                modalText={
                    isAdminCreatedModule
                        ? 'Are you sure you want to remove this module? You can re-add it later from the Home Defaults section when adding a new module.'
                        : 'Are you sure you want to remove this module? You can always create a new one later if needed.'
                }
                closeButtonText="Cancel"
                confirmButtonText="Confirm"
            />
        </>
    );
}
