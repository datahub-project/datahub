import { Text } from '@components';
import React, { useContext, useState } from 'react';
import styled from 'styled-components';

import AddElement from '@app/govern/Dashboard/Forms/AddElement';
import AddUsersModal from '@app/govern/Dashboard/Forms/AddUsersModal';
import ManageFormContext from '@app/govern/Dashboard/Forms/ManageFormContext';
import UsersList from '@app/govern/Dashboard/Forms/UsersList';
import { OwnershipCheckbox, StyledCheckbox } from '@app/govern/Dashboard/Forms/styledComponents';
import { useFormHandlers } from '@app/govern/Dashboard/Forms/useFormHandlers';
import InfoTooltip from '@src/app/sharedV2/icons/InfoTooltip';

const StyledText = styled.div`
    display: inline-flex;
    margin-left: 6px;
`;

const AddRecipients = () => {
    const { formValues } = useContext(ManageFormContext);
    const { handleOwnersCheckBox } = useFormHandlers();
    const [showUsersModal, setShowUsersModal] = useState<boolean>(false);

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
            <OwnershipCheckbox>
                <StyledCheckbox checked={formValues.actors?.owners} onChange={(e) => handleOwnersCheckBox(e)} />
                <Text color="gray">
                    Assign to Asset Owners
                    <StyledText>
                        <InfoTooltip content="Owners of the assigned assets will be requested to complete the form." />
                    </StyledText>
                </Text>
            </OwnershipCheckbox>
            <UsersList />
            <AddUsersModal showUsersModal={showUsersModal} setShowUsersModal={setShowUsersModal} />
        </>
    );
};

export default AddRecipients;
