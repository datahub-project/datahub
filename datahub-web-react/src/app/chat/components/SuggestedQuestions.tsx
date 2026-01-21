import { Pill } from '@components';
import React from 'react';
import styled from 'styled-components';

const SuggestionsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-top: 8px;
    justify-content: start;
    align-items: center;
    width: 100%;
`;

const PillsWrapper = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    justify-content: center;
`;

interface SuggestedQuestionsProps {
    onQuestionSelect: (question: string) => void;
    questions?: string[];
}

export const SuggestedQuestions: React.FC<SuggestedQuestionsProps> = ({ onQuestionSelect, questions }) => {
    if (!questions || !questions.length) {
        return null;
    }

    return (
        <SuggestionsContainer>
            <PillsWrapper>
                {questions.map((question) => (
                    <Pill
                        key={question}
                        label={question}
                        variant="filled"
                        color="violet"
                        clickable
                        onPillClick={() => onQuestionSelect(question)}
                    />
                ))}
            </PillsWrapper>
        </SuggestionsContainer>
    );
};
