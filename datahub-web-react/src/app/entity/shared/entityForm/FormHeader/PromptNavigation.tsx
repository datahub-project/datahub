import React from 'react';

import { message } from 'antd';
import styled from 'styled-components';
import { useEntityFormContext } from '../EntityFormContext';
import { ArrowLeft, ArrowRight, BulkNavigationWrapper, NavigationWrapper } from './components';
import { FormPromptType, SubmitFormPromptInput } from '../../../../../types.generated';
import StructuredPropertyPrompt from '../prompts/StructuredPropertyPrompt/StructuredPropertyPrompt';
import { useBatchSubmitFormPromptMutation } from '../../../../../graphql/form.generated';
import VerificationCTA from './VerificationCTA';
import { FORM_ANSWER_IN_BULK_ID } from '../../../../onboarding/config/FormOnboardingConfig';

const FormPromptsWrapper = styled(BulkNavigationWrapper)`
    justify-content: space-between;
`;

const RightColumn = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    margin-left: 16px;
`;

export default function PromptNavigation() {
    const {
        refetch,
        setShouldRefetch,
        prompt: {
            prompts,
            prompt,
            promptIndex,
            setSelectedPromptId,
        },
        entity: {
            selectedEntities,
            setSelectedEntities,
            setSubmittedEntities,
            setNumSubmittedEntities,
        }
    } = useEntityFormContext();

    const [batchSubmitFormPromptResponse] = useBatchSubmitFormPromptMutation();

    function navigateLeft() {
        if (prompts) {
            if (promptIndex === 0) {
                setSelectedPromptId(prompts?.[(prompts?.length || 0) - 1].id);
            } else {
                setSelectedPromptId(prompts?.[promptIndex - 1].id);
            }
            setNumSubmittedEntities(0);
        }
    }

    function navigateRight() {
        if (prompts) {
            if (promptIndex === (prompts?.length || 0) - 1) {
                setSelectedPromptId(prompts?.[0].id);
            } else {
                setSelectedPromptId(prompts?.[promptIndex + 1].id);
            }
            setNumSubmittedEntities(0);
        }
    }

    function submitResponse(promptInput: SubmitFormPromptInput, onSuccess: () => void) {
        message.loading('Submitting response...', 5000);
        batchSubmitFormPromptResponse({
            variables: { input: { assetUrns: selectedEntities.map((e) => e.urn), input: promptInput } },
        })
            .then(() => {
                refetch(); // 3 second wait already happens in refetch
                setTimeout(() => {
                    onSuccess();
                    setSubmittedEntities(selectedEntities);
                    setNumSubmittedEntities(selectedEntities.length);
                    setSelectedEntities([]);
                    setShouldRefetch(true);
                    message.destroy();
                }, 3000);
            })
            .catch(() => {
                message.error('Unknown error while batch submitting form response');
            });
    }

    return (
        <FormPromptsWrapper id={FORM_ANSWER_IN_BULK_ID}>
            {prompt?.type === FormPromptType.StructuredProperty && (
                <StructuredPropertyPrompt
                    key={promptIndex}
                    promptNumber={promptIndex + 1}
                    prompt={prompt}
                    submitResponse={submitResponse}
                />
            )}
            <RightColumn>
                <NavigationWrapper isHidden={!prompts?.length}>
                    <ArrowLeft onClick={navigateLeft} />
                    {promptIndex + 1}
                    &nbsp;of&nbsp;
                    {prompts?.length}
                    &nbsp;Questions
                    <ArrowRight onClick={navigateRight} />
                </NavigationWrapper>
                <VerificationCTA />
            </RightColumn>
        </FormPromptsWrapper>
    );
}
