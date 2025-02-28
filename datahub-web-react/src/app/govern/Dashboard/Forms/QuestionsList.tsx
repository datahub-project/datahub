import { DndContext, MouseSensor, TouchSensor, useSensor, useSensors } from '@dnd-kit/core';
import { restrictToVerticalAxis } from '@dnd-kit/modifiers';
import { arrayMove, SortableContext, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { FormPrompt } from '@src/types.generated';
import React, { useContext } from 'react';
import ManageFormContext from './ManageFormContext';
import QuestionCard from './QuestionCard';
import { CardsList } from './styledComponents';

interface Props {
    setShowQuestionModal: React.Dispatch<React.SetStateAction<boolean>>;
    setCurrentQuestion: React.Dispatch<React.SetStateAction<FormPrompt | undefined>>;
}

const QuestionsList = ({ setShowQuestionModal, setCurrentQuestion }: Props) => {
    const { formValues, setFormValues } = useContext(ManageFormContext);

    const sensors = useSensors(useSensor(MouseSensor), useSensor(TouchSensor));

    const handleDragEnd = (event: any) => {
        const { active, over } = event;
        if (active.id !== over.id) {
            setFormValues((prev) => {
                const oldIndex = formValues.questions.findIndex((ques) => ques.id === active.id);
                const newIndex = formValues.questions.findIndex((ques) => ques.id === over.id);
                const updatedQuestions = arrayMove(prev.questions, oldIndex, newIndex);

                return {
                    ...prev,
                    questions: updatedQuestions,
                };
            });
        }
    };

    return (
        <DndContext sensors={sensors} onDragEnd={handleDragEnd} modifiers={[restrictToVerticalAxis]}>
            <SortableContext items={formValues.questions.map((ques) => ques.id)} strategy={verticalListSortingStrategy}>
                <CardsList>
                    {formValues.questions?.map((question) => {
                        return (
                            <QuestionCard
                                question={question}
                                setShowQuestionModal={setShowQuestionModal}
                                setCurrentQuestion={setCurrentQuestion}
                                key={question.id}
                            />
                        );
                    })}
                </CardsList>
            </SortableContext>
        </DndContext>
    );
};

export default QuestionsList;
