import { message } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useEntityContext } from '../../EntityContext';
import { getBulkByQuestionPrompts, getFormAssociation } from '../../containers/profile/sidebar/FormInfo/utils';
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

interface Props {
    formUrn: string;
}

export default function PromptNavigation({ formUrn }: Props) {
    const [batchSubmitFormPromptResponse] = useBatchSubmitFormPromptMutation();
    const {
        selectedPromptId,
        selectedEntities,
        setSelectedPromptId,
        setSelectedEntities,
        setShouldRefetchSearchResults,
        refetchNumReadyForVerification,
    } = useEntityFormContext();
    const { entityData } = useEntityContext();
    const formAssociation = getFormAssociation(formUrn, entityData);
    const prompts = getBulkByQuestionPrompts(formUrn, entityData);
    const prompt = prompts.find((p) => p.id === selectedPromptId);
    const promptIndex = prompts.findIndex((p) => p.id === selectedPromptId);

    function navigateLeft() {
        if (promptIndex === 0) {
            setSelectedPromptId(prompts?.[(prompts?.length || 0) - 1].id);
        } else {
            setSelectedPromptId(prompts?.[promptIndex - 1].id);
        }
    }

    function navigateRight() {
        if (promptIndex === (prompts?.length || 0) - 1) {
            setSelectedPromptId(prompts?.[0].id);
        } else {
            setSelectedPromptId(prompts?.[promptIndex + 1].id);
        }
    }

    function submitResponse(promptInput: SubmitFormPromptInput, onSuccess: () => void) {
        batchSubmitFormPromptResponse({
            variables: { input: { assetUrns: selectedEntities.map((e) => e.urn), input: promptInput } },
        })
            .then(() => {
                onSuccess();
                message.loading('Submitting response...');
                setTimeout(() => {
                    setSelectedEntities([]);
                    setShouldRefetchSearchResults(true);
                    refetchNumReadyForVerification();
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
                <NavigationWrapper isHidden={!formAssociation}>
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
