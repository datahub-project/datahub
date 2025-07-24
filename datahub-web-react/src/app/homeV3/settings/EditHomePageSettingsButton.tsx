import { Button, Dropdown, colors } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

const ButtonWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: ${colors.white};
    height: 40px;
    width: 40px;
    position: fixed;
    right: 32px;
    bottom: 32px;
    border-radius: 200px;
    box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
`;

const DropdownContainer = styled.div`
    border-radius: 12px;
    box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
    background-color: white;
    overflow: hidden; // Cleanly rounds edges

    .ant-dropdown-menu-item {
        padding: 8px 16px;
    }
`;

export default function EditHomePageSettingsButton() {
    const user = useUserContext();
    const canEditDefaultTemplate = user.platformPrivileges?.manageHomePageTemplates;

    const { setIsEditingGlobalTemplate, isEditingGlobalTemplate, resetTemplateToDefault, personalTemplate } =
        usePageTemplateContext();

    const isOnPersonalTemplate = !!personalTemplate;

    const [showConfirmResetModal, setShowConfirmResetModal] = useState(false);

    if (isEditingGlobalTemplate || (!canEditDefaultTemplate && !isOnPersonalTemplate)) return null;

    const handleResetToDefault = () => {
        resetTemplateToDefault();
        setShowConfirmResetModal(false);
    };

    const menu = {
        items: [
            ...(canEditDefaultTemplate
                ? [
                      {
                          label: 'Edit Organization Default',
                          key: 'edit-organization-default',
                          style: {
                              color: colors.gray[600],
                              fontSize: '14px',
                          },
                          onClick: () => setIsEditingGlobalTemplate(true),
                      },
                  ]
                : []),
            ...(isOnPersonalTemplate
                ? [
                      {
                          label: 'Reset to Organization Default',
                          key: 'reset-to-organization-default',
                          style: {
                              color: colors.red[1000],
                              fontSize: '14px',
                          },
                          onClick: () => setShowConfirmResetModal(true),
                      },
                  ]
                : []),
        ],
    };

    return (
        <>
            <ButtonWrapper>
                <Dropdown
                    menu={menu}
                    trigger={['click']}
                    dropdownRender={(menuNode) => <DropdownContainer>{menuNode}</DropdownContainer>}
                >
                    <Button icon={{ icon: 'Gear', color: 'gray', source: 'phosphor', size: '4xl' }} variant="text" />
                </Dropdown>
            </ButtonWrapper>
            <ConfirmationModal
                isOpen={!!showConfirmResetModal}
                handleConfirm={handleResetToDefault}
                handleClose={() => setShowConfirmResetModal(false)}
                modalTitle="Confirm reset to default template"
                modalText="Are you sure you want to reset your homepage to the organization's default template? You will lose all your personal modules."
                closeButtonText="Cancel"
                confirmButtonText="Confirm"
            />
        </>
    );
}
