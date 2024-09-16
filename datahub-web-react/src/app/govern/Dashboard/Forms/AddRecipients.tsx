import { Text } from '@components';
import InfoTooltip from '@src/app/sharedV2/icons/InfoTooltip';
import { Checkbox } from 'antd';
import styled from 'styled-components';
import React, { useContext, useState } from 'react';
import AddElement from './AddElement';
import AddUsersModal from './AddUsersModal';
import ManageFormContext from './ManageFormContext';
import { OwnershipCheckbox } from './styledComponents';
import { useFormHandlers } from './useFormHandlers';
import UsersList from './UsersList';

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
                description="Select the users and groups who will complete this form"
                buttonLabel="Add Users or Groups"
                buttonOnClick={() => setShowUsersModal(true)}
                buttonTooltip="Assign specific users or groups"
            />
            <OwnershipCheckbox>
                <Checkbox checked={formValues.actors?.owners} onChange={(e) => handleOwnersCheckBox(e)} />
                <Text size="lg" color="gray">
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
