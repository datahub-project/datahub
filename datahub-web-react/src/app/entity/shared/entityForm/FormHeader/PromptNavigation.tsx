import { CheckCircleFilled } from '@ant-design/icons';
import { message, notification } from 'antd';
import * as QueryString from 'query-string';
import React from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import analytics, { DocRequestView, EventType } from '@app/analytics';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import VerificationCTA from '@app/entity/shared/entityForm/FormHeader/VerificationCTA';
import {
    ArrowLeft,
    ArrowRight,
    BulkNavigationWrapper,
    NavigationWrapper,
} from '@app/entity/shared/entityForm/FormHeader/components';
import DocumentationPrompt from '@app/entity/shared/entityForm/prompts/DocumentationPrompt/DocumentationPrompt';
import DomainPrompt from '@app/entity/shared/entityForm/prompts/DomainPrompt/DomainPrompt';
import GlossaryTermsPrompt from '@app/entity/shared/entityForm/prompts/GlossaryTermsPrompt/GlossaryTermsPrompt';
import OwnershipPrompt from '@app/entity/shared/entityForm/prompts/OwnershipPrompt/OwnershipPrompt';
import StructuredPropertyPrompt from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/StructuredPropertyPrompt';
import { FORM_ANSWER_IN_BULK_ID } from '@app/onboarding/config/FormOnboardingConfig';
import { pluralize } from '@app/shared/textUtil';
import useGetSearchQueryInputs from '@src/app/search/useGetSearchQueryInputs';

import { useAsyncBatchSubmitFormPromptMutation, useBatchSubmitFormPromptMutation } from '@graphql/form.generated';
import { EntityType, FormPromptType, SubmitFormPromptInput } from '@types';

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
    const history = useHistory();
    const location = useLocation();
    const { query } = useGetSearchQueryInputs();
    const {
        form: { isVerificationType },
        submission: { handlePromptSubmission, handleUndoPromptSubmission, handleAsyncBatchSubmit },
        prompt: { prompts, prompt, promptIndex, setSelectedPromptId },
        entity: {
            selectedEntities,
            setSelectedEntities,
            setNumSubmittedEntities,
            areAllEntitiesSelected,
            setAreAllEntitiesSelected,
        },
        filter: { formFilter, orFilters },
        search: { results },
    } = useEntityFormContext();

    const [batchSubmitFormPromptResponse] = useBatchSubmitFormPromptMutation();
    const [asyncBatchSubmitFormPromptResponse] = useAsyncBatchSubmitFormPromptMutation();
    const isOnLastPrompt = promptIndex === (prompts?.length || 0) - 1;

    function resetPage() {
        const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
        history.push({ search: QueryString.stringify({ ...params, page: 0 }) });
    }

    function navigateLeft() {
        if (prompts) {
            if (promptIndex === 0) {
                setSelectedPromptId(prompts?.[(prompts?.length || 0) - 1].id);
            } else {
                setSelectedPromptId(prompts?.[promptIndex - 1].id);
            }
            setNumSubmittedEntities(0);
            resetPage();
        }
    }

    function navigateRight() {
        if (prompts) {
            if (isOnLastPrompt) {
                setSelectedPromptId(prompts?.[0].id);
            } else {
                setSelectedPromptId(prompts?.[promptIndex + 1].id);
            }
            setNumSubmittedEntities(0);
            resetPage();
        }
    }

    function batchSubmit(promptInput: SubmitFormPromptInput, onSuccess: () => void) {
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

    function asyncBatchSubmit(promptInput: SubmitFormPromptInput, onSuccess: () => void) {
        const types = results.searchAcrossEntities?.facets
            ?.find((f) => f.field === '_entityType')
            ?.aggregations.filter((agg) => !!agg.count)
            .map((agg) => agg.value) as EntityType[];
        const totalCount = results.searchAcrossEntities?.total || 0;
        asyncBatchSubmitFormPromptResponse({
            variables: {
                input: {
                    types: types || [],
                    formFilter,
                    orFilters,
                    query,
                    input: promptInput,
                    taskInput: { taskName: `Question ${promptIndex + 1}` },
                },
            },
        })
            .then((result) => {
                analytics.event({
                    type: EventType.CompleteDocRequestPrompt,
                    source: DocRequestView.ByAsset,
                    required: prompt?.required as boolean,
                    promptId: promptInput.promptId,
                    numAssets: totalCount,
                });
                message.destroy();
                notification.success({
                    message: 'Success',
                    description: `Started task to submit a response for ${totalCount} ${pluralize(
                        selectedEntities.length,
                        'asset',
                    )}.`,
                    placement: 'bottomLeft',
                    duration: 3,
                    icon: <CheckCircleFilled style={{ color: '#078781' }} />,
                });
                setSelectedEntities([]);
                setAreAllEntitiesSelected(false);
                onSuccess();
                if (result.data?.asyncBatchSubmitFormPrompt.taskUrn) {
                    handleAsyncBatchSubmit(result.data?.asyncBatchSubmitFormPrompt.taskUrn, promptInput.promptId);
                }
            })
            .catch(() => {
                message.destroy();
                message.error('Unknown error while batch submitting form response');
            });
    }

    function submitResponse(promptInput: SubmitFormPromptInput, onSuccess: () => void) {
        message.loading('Submitting response...');
        if (areAllEntitiesSelected) {
            asyncBatchSubmit(promptInput, onSuccess);
        } else {
            batchSubmit(promptInput, onSuccess);
        }
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
            {prompt?.type === FormPromptType.Documentation && (
                <DocumentationPrompt
                    key={promptIndex}
                    promptNumber={promptIndex + 1}
                    prompt={prompt}
                    submitResponse={submitResponse}
                />
            )}
            {prompt?.type === FormPromptType.GlossaryTerms && (
                <GlossaryTermsPrompt
                    key={promptIndex}
                    promptNumber={promptIndex + 1}
                    prompt={prompt}
                    submitResponse={submitResponse}
                />
            )}
            {prompt?.type === FormPromptType.Domain && (
                <DomainPrompt
                    key={promptIndex}
                    promptNumber={promptIndex + 1}
                    prompt={prompt}
                    submitResponse={submitResponse}
                />
            )}
            <RightColumn>
                <NavigationWrapper isHidden={!prompts?.length}>
                    <ArrowLeft onClick={navigateLeft} $shouldHide={promptIndex === 0} />
                    {promptIndex + 1}
                    &nbsp;of&nbsp;
                    {prompts?.length}
                    &nbsp;Questions
                    <ArrowRight onClick={navigateRight} $shouldHide={isOnLastPrompt} />
                </NavigationWrapper>
                {isVerificationType && <VerificationCTA />}
            </RightColumn>
        </FormPromptsWrapper>
    );
}
