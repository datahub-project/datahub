import { Text } from '@src/alchemy-components';
import { Checkbox, Divider } from 'antd';
import React, { useState } from 'react';
import AddElement from './AddElement';
import AddQuestionModal from './AddQuestionModal';
import { FormQuestion } from './formUtils';
import QuestionsList from './QuestionsList';
import { OwnershipCheckbox } from './styledComponents';

const AddToForm = () => {
    const [showQuestionModal, setShowQuestionModal] = useState<boolean>(false);
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
            />
            <OwnershipCheckbox>
                <Checkbox />
                <Text size="lg" color="gray">
                    Asset owners will be prompted to fill out this form.
                </Text>
            </OwnershipCheckbox>
            <AddQuestionModal
                showQuestionModal={showQuestionModal}
                setShowQuestionModal={setShowQuestionModal}
                question={currentQuestion}
                setCurrentQuestion={setCurrentQuestion}
            />
        </>
    );
};

export default AddToForm;
