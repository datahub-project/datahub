import { Button, Icon, Text } from '@components';
import { useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { FormState } from '@src/types.generated';
import React, { useContext, useState } from 'react';
import { FormQuestion, questionTypes } from './formUtils';
import ManageFormContext from './ManageFormContext';
import { CardContainer, CardData, CardIcons, DragIcon, MandatoryTag } from './styledComponents';

interface Props {
    question: FormQuestion;
    setShowQuestionModal: React.Dispatch<React.SetStateAction<boolean>>;
    setCurrentQuestion: React.Dispatch<React.SetStateAction<FormQuestion | undefined>>;
}

const QuestionCard = ({ question, setShowQuestionModal, setCurrentQuestion }: Props) => {
    const { attributes, listeners, setNodeRef, transform, isDragging } = useSortable({
        id: question.id,
    });
    const { setFormValues, formValues } = useContext(ManageFormContext);
    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);

    const handleDeleteQuestion = () => {
        setFormValues((prev) => ({
            ...prev,
            questions: prev.questions?.filter((ques) => ques.id !== question.id),
        }));
        setShowConfirmDelete(false);
    };

    const handleDeleteClose = () => {
        setShowConfirmDelete(false);
    };

    return (
        <>
            <CardContainer
                ref={setNodeRef}
                {...attributes}
                isDragging={isDragging}
                transform={CSS.Transform.toString(transform)}
            >
                {formValues.state === FormState.Draft && (
                    <DragIcon {...listeners} size="lg" color="gray" icon="DragIndicator" isDragging={isDragging} />
                )}
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
                {formValues.state === FormState.Draft ? (
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
                ) : (
                    <Button
                        variant="text"
                        onClick={() => {
                            setCurrentQuestion(question);
                            setShowQuestionModal(true);
                        }}
                    >
                        View
                    </Button>
                )}
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
