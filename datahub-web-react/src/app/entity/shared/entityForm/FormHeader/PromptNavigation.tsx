import React from 'react';

import { CheckCircleFilled } from '@ant-design/icons';
import { message, notification } from 'antd';
import styled from 'styled-components';
import { useEntityFormContext } from '../EntityFormContext';
import { ArrowLeft, ArrowRight, BulkNavigationWrapper, NavigationWrapper } from './components';
import { FormPromptType, SubmitFormPromptInput } from '../../../../../types.generated';
import StructuredPropertyPrompt from '../prompts/StructuredPropertyPrompt/StructuredPropertyPrompt';
import { useBatchSubmitFormPromptMutation } from '../../../../../graphql/form.generated';
import VerificationCTA from './VerificationCTA';
import { FORM_ANSWER_IN_BULK_ID } from '../../../../onboarding/config/FormOnboardingConfig';
import { pluralize } from '../../../../shared/textUtil';
import analytics, { EventType, DocRequestView } from '../../../../analytics';
import OwnershipPrompt from '../prompts/OwnershipPrompt/OwnershipPrompt';

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
        form: { isVerificationType },
        submission: { handlePromptSubmission, handleUndoPromptSubmission },
        prompt: { prompts, prompt, promptIndex, setSelectedPromptId },
        entity: { selectedEntities, setSelectedEntities, setNumSubmittedEntities },
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
        message.loading('Submitting response...');
        const selectedEntityUrns = selectedEntities.map((e) => e.urn);
        batchSubmitFormPromptResponse({
            variables: { input: { assetUrns: selectedEntityUrns, input: promptInput } },
        })
            .then(() => {
                analytics.event({
                    type: EventType.CompleteDocRequestPrompt,
                    source: DocRequestView.ByAsset,
                    required: prompt?.required as boolean,
                    promptId: promptInput.promptId,
                    numAssets: selectedEntities.length,
                });
                handlePromptSubmission(promptInput.promptId, selectedEntityUrns);
                message.destroy();
                notification.success({
                    message: 'Success',
                    description: `You have successfully submitted a response for ${selectedEntities.length} ${pluralize(
                        selectedEntities.length,
                        'asset',
                    )}.`,
                    placement: 'bottomLeft',
                    duration: 3,
                    icon: <CheckCircleFilled style={{ color: '#078781' }} />,
                });
                setSelectedEntities([]);
                onSuccess();
            })
            .catch(() => {
                handleUndoPromptSubmission(promptInput.promptId, selectedEntityUrns);
                message.destroy();
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
            {prompt?.type === FormPromptType.Ownership && (
                <OwnershipPrompt
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
                {isVerificationType && <VerificationCTA />}
            </RightColumn>
        </FormPromptsWrapper>
    );
}
