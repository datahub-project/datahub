import { Text } from '@src/alchemy-components';
import { Checkbox, Divider } from 'antd';
import React, { useContext, useState } from 'react';
import AddElement from './AddElement';
import AddQuestionModal from './AddQuestionModal';
import AddUsersModal from './AddUsersModal';
import { FormQuestion } from './formUtils';
import ManageFormContext from './ManageFormContext';
import QuestionsList from './QuestionsList';
import { OwnershipCheckbox } from './styledComponents';
import { useFormHandlers } from './useFormHandlers';
import UsersList from './UsersList';

const AddToForm = () => {
    const { formValues } = useContext(ManageFormContext);
    const { handleOwnersCheckBox } = useFormHandlers();

    const [showQuestionModal, setShowQuestionModal] = useState<boolean>(false);
    const [showUsersModal, setShowUsersModal] = useState<boolean>(false);

    const [currentQuestion, setCurrentQuestion] = useState<FormQuestion | undefined>();

    return (
        <>
            <AddElement
                heading="Add Questions"
                description="Add some questions"
                buttonLabel="Add Questions"
                buttonOnClick={() => setShowQuestionModal(true)}
            />
            <QuestionsList setShowQuestionModal={setShowQuestionModal} setCurrentQuestion={setCurrentQuestion} />

            <Divider />
            <AddElement
                heading="Assign Assets"
                description="Assign the Assets for which you want to collect the data"
                buttonLabel="Add Assets"
            />
            <Divider />

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
            <AddQuestionModal
                showQuestionModal={showQuestionModal}
                setShowQuestionModal={setShowQuestionModal}
                question={currentQuestion}
                setCurrentQuestion={setCurrentQuestion}
            />
            <AddUsersModal showUsersModal={showUsersModal} setShowUsersModal={setShowUsersModal} />
        </>
    );
};

export default AddToForm;
