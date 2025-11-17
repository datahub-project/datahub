import { Pill } from '@components';
import React from 'react';
import styled from 'styled-components';

const SuggestionsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-top: 24px;
    justify-content: start;
    align-items: center;
    width: 100%;
`;

const PillsWrapper = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    justify-content: start;
`;

interface SuggestedQuestionsProps {
    onQuestionSelect: (question: string) => void;
}

const DEFAULT_QUESTIONS = [
    'Show the most highly used tables',
    'Find reports related to sales',
    'Help me build a new dashboard',
];

export const SuggestedQuestions: React.FC<SuggestedQuestionsProps> = ({ onQuestionSelect }) => {
    return (
        <SuggestionsContainer>
            <PillsWrapper>
                {DEFAULT_QUESTIONS.map((question) => (
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
