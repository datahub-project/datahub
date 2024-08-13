import { Icon, Text } from '@src/alchemy-components';
import React from 'react';
import { FormQuestion, questionTypes } from './formUtils';
import { CardContainer, CardData, CardIcons, MandatoryTag } from './styledComponents';

interface Props {
    question: FormQuestion;
    setShowQuestionModal: React.Dispatch<React.SetStateAction<boolean>>;
    setCurrentQuestion: React.Dispatch<React.SetStateAction<FormQuestion | undefined>>;
}

const QuestionCard = ({ question, setShowQuestionModal, setCurrentQuestion }: Props) => {
    return (
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
                <Icon icon="Delete" size="md" />
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
    );
};

export default QuestionCard;
