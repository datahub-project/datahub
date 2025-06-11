import { Text } from '@components';
import React, { useContext, useState } from 'react';
import styled from 'styled-components';

import AddElement from '@app/govern/Dashboard/Forms/AddElement';
import AddUsersModal from '@app/govern/Dashboard/Forms/AddUsersModal';
import ManageFormContext from '@app/govern/Dashboard/Forms/ManageFormContext';
import UsersList from '@app/govern/Dashboard/Forms/UsersList';
import { AddRecipientsOptions, OptionCheckbox, StyledCheckbox } from '@app/govern/Dashboard/Forms/styledComponents';
import { useFormHandlers } from '@app/govern/Dashboard/Forms/useFormHandlers';
import { useAppConfig } from '@app/useAppConfig';
import InfoTooltip from '@src/app/sharedV2/icons/InfoTooltip';

const StyledText = styled.div`
    display: inline-flex;
    margin-left: 6px;
`;

const AddRecipients = () => {
    const { config } = useAppConfig();
    const { formValues } = useContext(ManageFormContext);
    const { handleOwnersCheckBox, handleNotifyAsigneesCheckBox } = useFormHandlers();
    const [showUsersModal, setShowUsersModal] = useState<boolean>(false);

    const { formsNotificationsEnabled } = config.featureFlags;

    return (
        <>
            <AddElement
                heading="Add Recipients"
                description="Select users and groups who will be required to complete this form."
                buttonLabel="Add Users or Groups"
                buttonOnClick={() => setShowUsersModal(true)}
                buttonTooltip="Assign specific users or groups"
                dataTestIdPrefix="add-recipients"
            />
            <AddRecipientsOptions>
                <OptionCheckbox>
                    <StyledCheckbox checked={formValues.actors?.owners} onChange={(e) => handleOwnersCheckBox(e)} />
                    <Text color="gray">
                        Assign to Asset Owners
                        <StyledText>
                            <InfoTooltip content="Owners of the assigned assets will be requested to complete the form." />
                        </StyledText>
                    </Text>
                </OptionCheckbox>

                {formsNotificationsEnabled && (
                    <OptionCheckbox>
                        <StyledCheckbox
                            checked={!!formValues.formSettings?.notificationSettings?.notifyAssigneesOnPublish}
                            onChange={(e) => handleNotifyAsigneesCheckBox(e.target.checked)}
                            data-testid="notify-assignees-on-publish-checkbox"
                        />
                        <Text color="gray">
                            Send notifications to assignees when this form is published
                            <StyledText>
                                <InfoTooltip content="Users can be notified via email or slack. All users of assigned groups will be notified." />
                            </StyledText>
                        </Text>
                    </OptionCheckbox>
                )}
            </AddRecipientsOptions>

            <UsersList />
            <AddUsersModal showUsersModal={showUsersModal} setShowUsersModal={setShowUsersModal} />
        </>
    );
};

export default AddRecipients;
