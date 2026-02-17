import { Dropdown } from '@components';
import React, { useCallback, useState } from 'react';
import { createPortal } from 'react-dom';
import styled, { useTheme } from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { StyledIcon } from '@app/homeV3/styledComponents';
import { ANT_NOTIFICATION_Z_INDEX } from '@app/shared/constants';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

const ButtonWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: ${(props) => props.theme.colors.bg};
    height: 40px;
    width: 40px;
    position: fixed;
    right: 32px;
    bottom: 32px;
    border-radius: 200px;
    box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
    z-index: ${ANT_NOTIFICATION_Z_INDEX + 1};
`;

const DropdownContainer = styled.div`
    border-radius: 12px;
    box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
    background-color: ${(props) => props.theme.colors.bgSurface};
    overflow: hidden; // Cleanly rounds edges

    .ant-dropdown-menu-item {
        padding: 8px 16px;
    }
`;

export default function EditHomePageSettingsButton() {
    const theme = useTheme();
    const user = useUserContext();
    const canEditDefaultTemplate = user.platformPrivileges?.manageHomePageTemplates;

    const { setIsEditingGlobalTemplate, isEditingGlobalTemplate, resetTemplateToDefault, personalTemplate } =
        usePageTemplateContext();

    const isOnPersonalTemplate = !!personalTemplate;

    const [showConfirmResetModal, setShowConfirmResetModal] = useState(false);

    const startGlobalTemplateEdit = useCallback(() => {
        setIsEditingGlobalTemplate(true);
        analytics.event({
            type: EventType.HomePageTemplateGlobalTemplateEditingStart,
        });
    }, [setIsEditingGlobalTemplate]);

    const handleResetToDefault = useCallback(() => {
        resetTemplateToDefault();
        setShowConfirmResetModal(false);
        analytics.event({
            type: EventType.HomePageTemplateResetToGlobalTemplate,
        });
    }, [resetTemplateToDefault]);

    if (isEditingGlobalTemplate || (!canEditDefaultTemplate && !isOnPersonalTemplate)) return null;

    const menu = {
        items: [
            ...(canEditDefaultTemplate
                ? [
                      {
                          label: 'Edit Organization Default',
                          key: 'edit-organization-default',
                          style: {
                              color: theme.colors.text,
                              fontSize: '14px',
                          },
                          onClick: startGlobalTemplateEdit,
                          'data-testid': 'edit-organization-default',
                      },
                  ]
                : []),
            ...(isOnPersonalTemplate
                ? [
                      {
                          label: 'Reset to Organization Default',
                          key: 'reset-to-organization-default',
                          style: {
                              color: theme.colors.textError,
                              fontSize: '14px',
                          },
                          onClick: () => setShowConfirmResetModal(true),
                          'data-testid': 'reset-to-organization-default',
                      },
                  ]
                : []),
        ],
    };

    return (
        <>
            {createPortal(
                <ButtonWrapper>
                    <Dropdown
                        menu={menu}
                        trigger={['click']}
                        dropdownRender={(menuNode) => <DropdownContainer>{menuNode}</DropdownContainer>}
                    >
                        <StyledIcon
                            icon="Gear"
                            source="phosphor"
                            size="4xl"
                            data-testid="edit-home-page-settings"
                        />
                    </Dropdown>
                </ButtonWrapper>,
                document.body,
            )}
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
