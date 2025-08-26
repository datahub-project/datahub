import { Button, Icon, Text, Tooltip } from '@components';
import { useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import React, { useContext, useState } from 'react';

import ManageFormContext from '@app/govern/Dashboard/Forms/ManageFormContext';
import { questionTypes } from '@app/govern/Dashboard/Forms/formUtils';
import {
    CardContainer,
    CardData,
    CardIcons,
    DragIcon,
    MandatoryTag,
} from '@app/govern/Dashboard/Forms/styledComponents';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { FormPrompt, FormState } from '@src/types.generated';

interface Props {
    question: FormPrompt;
    setShowQuestionModal: React.Dispatch<React.SetStateAction<boolean>>;
    setCurrentQuestion: React.Dispatch<React.SetStateAction<FormPrompt | undefined>>;
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
                        <Tooltip title="Delete question" showArrow={false}>
                            <Icon icon="Delete" size="md" onClick={() => setShowConfirmDelete(true)} />
                        </Tooltip>
                        <Tooltip title="Edit question" showArrow={false}>
                            <Icon
                                icon="Edit"
                                size="md"
                                onClick={() => {
                                    setCurrentQuestion(question);
                                    setShowQuestionModal(true);
                                }}
                            />
                        </Tooltip>
                    </CardIcons>
                ) : (
                    <Tooltip
                        title=" Questions cannot be edited once a form has been published. To edit questions create a new compliance form."
                        showArrow={false}
                    >
                        <Button
                            variant="text"
                            onClick={() => {
                                setCurrentQuestion(question);
                                setShowQuestionModal(true);
                            }}
                        >
                            View
                        </Button>
                    </Tooltip>
                )}
            </CardContainer>
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleDeleteClose}
                handleConfirm={handleDeleteQuestion}
                modalTitle="Confirm Delete"
                modalText="Are you sure you want to delete the question?"
            />
        </>
    );
};

export default QuestionCard;
