import { Icon, Text } from '@src/alchemy-components';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import React, { useContext, useState } from 'react';
import { FormQuestion, questionTypes } from './formUtils';
import ManageFormContext from './ManageFormContext';
import { CardContainer, CardData, CardIcons, MandatoryTag } from './styledComponents';

interface Props {
    question: FormQuestion;
    setShowQuestionModal: React.Dispatch<React.SetStateAction<boolean>>;
    setCurrentQuestion: React.Dispatch<React.SetStateAction<FormQuestion | undefined>>;
}

const QuestionCard = ({ question, setShowQuestionModal, setCurrentQuestion }: Props) => {
    const { setFormValues } = useContext(ManageFormContext);

    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);

    const handleDeleteQuestion = () => {
        setFormValues((prev) => ({
            ...prev,
            questions: prev.questions?.filter((ques) => ques !== question),
        }));
        setShowConfirmDelete(false);
    };

    const handleDeleteClose = () => {
        setShowConfirmDelete(false);
    };

    return (
        <>
            <CardContainer>
                <CardData width="30%">
                    <Text color="gray" weight="medium">
                        {question.title}
                    </Text>
                </CardData>
                <CardData width="30%">
                    <Text color="gray">
                        {question.type && questionTypes.find((type) => type.value === question.type)?.label}
                    </Text>
                </CardData>
                <CardData width="20%">{question.required ? <MandatoryTag> Mandatory</MandatoryTag> : <></>}</CardData>
                <CardIcons>
                    <Icon icon="Delete" size="md" onClick={() => setShowConfirmDelete(true)} />
                    <Icon
                        icon="Edit"
                        size="md"
                        onClick={() => {
                            setCurrentQuestion(question);
                            setShowQuestionModal(true);
                        }}
                    />
                </CardIcons>
            </CardContainer>
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleDeleteClose}
                handleConfirm={handleDeleteQuestion}
                modalTitle="Confirm Delete"
                modalText="Are you sure you want to delete the question?"
                isDeleteModal
            />
        </>
    );
};

export default QuestionCard;
