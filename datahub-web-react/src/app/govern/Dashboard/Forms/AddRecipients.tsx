import { Text } from '@components';
import { Checkbox } from 'antd';
import React, { useContext, useState } from 'react';
import AddElement from './AddElement';
import AddUsersModal from './AddUsersModal';
import ManageFormContext from './ManageFormContext';
import { OwnershipCheckbox } from './styledComponents';
import { useFormHandlers } from './useFormHandlers';
import UsersList from './UsersList';

const AddRecipients = () => {
    const { formValues } = useContext(ManageFormContext);
    const { handleOwnersCheckBox } = useFormHandlers();
    const [showUsersModal, setShowUsersModal] = useState<boolean>(false);

    return (
        <>
            <AddElement
                heading="Add Recipients"
                description="Add Users and Groups to collect the data from"
                buttonLabel="Add Users"
                buttonOnClick={() => setShowUsersModal(true)}
            />
            <OwnershipCheckbox>
                <Checkbox checked={formValues.actors?.owners} onChange={(e) => handleOwnersCheckBox(e)} />
                <Text size="lg" color="gray">
                    Asset owners will be prompted to fill out this form.
                </Text>
            </OwnershipCheckbox>
            <UsersList />
            <AddUsersModal showUsersModal={showUsersModal} setShowUsersModal={setShowUsersModal} />
        </>
    );
};

export default AddRecipients;
