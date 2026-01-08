import { Pill } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useIsFreeTrialInstance } from '@app/useAppConfig';

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
    justify-content: center;
`;

interface SuggestedQuestionsProps {
    onQuestionSelect: (question: string) => void;
    questions?: string[];
}

const DEFAULT_QUESTIONS = [
    'Show the most highly used tables',
    'Find reports related to sales',
    'Help me build a new dashboard',
];

const DEFAULT_FREE_TRIAL_INSTANCE_QUESTIONS = [
    'How are orders with promotions calculated?',
    'How can I see trends in orders?',
    'What would be impacted if I redefined product_categories in postgres?',
];

export const SuggestedQuestions: React.FC<SuggestedQuestionsProps> = ({ onQuestionSelect, questions }) => {
    const isFreeTrialInstance = useIsFreeTrialInstance();

    let questionsToRender;

    if (questions && questions.length > 0) questionsToRender = questions;
    else if (isFreeTrialInstance) {
        questionsToRender = DEFAULT_FREE_TRIAL_INSTANCE_QUESTIONS;
    } else {
        questionsToRender = DEFAULT_QUESTIONS;
    }

    return (
        <SuggestionsContainer>
            <PillsWrapper>
                {questionsToRender.map((question) => (
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
