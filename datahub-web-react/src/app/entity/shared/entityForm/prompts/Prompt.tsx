import { message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import {
    FormPrompt as PromptEntity,
    FormPromptType,
    SubmitFormPromptInput,
    SchemaField,
} from '../../../../../types.generated';
import StructuredPropertyPrompt from './StructuredPropertyPrompt/StructuredPropertyPrompt';
import { useSubmitFormPromptMutation } from '../../../../../graphql/form.generated';
import { useEntityContext, useMutationUrn } from '../../EntityContext';
import analytics, { EventType, DocRequestView } from '../../../../analytics';
import useColumnSelector from './useColumnSelector';
import OwnershipPrompt from './OwnershipPrompt/OwnershipPrompt';
import DocumentationPrompt from './DocumentationPrompt/DocumentationPrompt';
import GlossaryTermsPrompt from './GlossaryTermsPrompt/GlossaryTermsPrompt';
import DomainPrompt from './DomainPrompt/DomainPrompt';

export const PromptWrapper = styled.div`
    background-color: white;
    border-radius: 8px;
    padding: 24px;
    margin-bottom: 8px;
`;

const MAX_NUM_FIELDS_TO_SUBMIT_SYNCHRONOUSLY = 10;

interface Props {
    promptNumber?: number;
    prompt: PromptEntity;
    field?: SchemaField;
    associatedUrn?: string;
    schemaFields?: SchemaField[];
}

export default function Prompt({ promptNumber, prompt, field, associatedUrn, schemaFields }: Props) {
    const { refetch } = useEntityContext();
    const [optimisticCompletedTimestamp, setOptimisticCompletedTimestamp] = useState<number | null>(null);
    const columnSelectorProps = useColumnSelector(schemaFields);
    const { isBulkApplyingFieldPath, selectedFieldPaths, setIsBulkApplyingFieldPath, setSelectedFieldPaths } =
        columnSelectorProps;
    const [submitFormPrompt] = useSubmitFormPromptMutation();
    const urn = useMutationUrn();

    function submitResponse(input: SubmitFormPromptInput, onSuccess: () => void) {
        submitFormPrompt({ variables: { urn: associatedUrn || urn, input } })
            .then(() => {
                onSuccess();
                setOptimisticCompletedTimestamp(Date.now());
                refetch();
                analytics.event({
                    type: EventType.CompleteDocRequestPrompt,
                    source: DocRequestView.ByAsset,
                    required: prompt.required,
                    promptId: prompt.id,
                    numAssets: 1,
                });
                setIsBulkApplyingFieldPath(false);
                setSelectedFieldPaths([]);
            })
            .catch(() => {
                message.error('Unknown error while submitting form response');
            });
    }

    function handleSubmitResponse(input: SubmitFormPromptInput, onSuccess: () => void) {
        let updatedInput = input;
        if (isBulkApplyingFieldPath && selectedFieldPaths.length <= MAX_NUM_FIELDS_TO_SUBMIT_SYNCHRONOUSLY) {
            updatedInput = {
                ...input,
                fieldPaths: selectedFieldPaths,
            };
        }
        submitResponse(updatedInput, onSuccess);
        // delay submitting many fields in a separate call for snappier response on initial submit
        if (isBulkApplyingFieldPath && selectedFieldPaths.length > MAX_NUM_FIELDS_TO_SUBMIT_SYNCHRONOUSLY) {
            updatedInput = {
                ...input,
                fieldPaths: selectedFieldPaths,
            };
            submitFormPrompt({ variables: { urn: associatedUrn || urn, input: updatedInput } })
                .then(() => {
                    onSuccess();
                    refetch();
                })
                .catch(() => {
                    message.error('Unknown error while submitting form response');
                });
        }
    }

    return (
        <PromptWrapper>
            {prompt.type === FormPromptType.StructuredProperty && (
                <StructuredPropertyPrompt
                    promptNumber={promptNumber}
                    prompt={prompt}
                    submitResponse={handleSubmitResponse}
                    optimisticCompletedTimestamp={optimisticCompletedTimestamp}
                />
            )}
            {prompt.type === FormPromptType.FieldsStructuredProperty && (
                <StructuredPropertyPrompt
                    promptNumber={promptNumber}
                    prompt={prompt}
                    submitResponse={handleSubmitResponse}
                    field={field}
                    optimisticCompletedTimestamp={optimisticCompletedTimestamp}
                    columnSelectorProps={columnSelectorProps}
                />
            )}
            {prompt.type === FormPromptType.Ownership && (
                <OwnershipPrompt
                    promptNumber={promptNumber}
                    prompt={prompt}
                    submitResponse={handleSubmitResponse}
                    optimisticCompletedTimestamp={optimisticCompletedTimestamp}
                />
            )}
            {prompt.type === FormPromptType.Documentation && (
                <DocumentationPrompt
                    promptNumber={promptNumber}
                    prompt={prompt}
                    submitResponse={handleSubmitResponse}
                    optimisticCompletedTimestamp={optimisticCompletedTimestamp}
                />
            )}
            {prompt.type === FormPromptType.FieldsDocumentation && (
                <DocumentationPrompt
                    promptNumber={promptNumber}
                    prompt={prompt}
                    submitResponse={handleSubmitResponse}
                    field={field}
                    optimisticCompletedTimestamp={optimisticCompletedTimestamp}
                    columnSelectorProps={columnSelectorProps}
                />
            )}
            {prompt.type === FormPromptType.GlossaryTerms && (
                <GlossaryTermsPrompt
                    promptNumber={promptNumber}
                    prompt={prompt}
                    submitResponse={handleSubmitResponse}
                    optimisticCompletedTimestamp={optimisticCompletedTimestamp}
                />
            )}
            {prompt.type === FormPromptType.FieldsGlossaryTerms && (
                <GlossaryTermsPrompt
                    promptNumber={promptNumber}
                    prompt={prompt}
                    submitResponse={handleSubmitResponse}
                    field={field}
                    optimisticCompletedTimestamp={optimisticCompletedTimestamp}
                    columnSelectorProps={columnSelectorProps}
                />
            )}
            {prompt.type === FormPromptType.Domain && (
                <DomainPrompt
                    promptNumber={promptNumber}
                    prompt={prompt}
                    submitResponse={handleSubmitResponse}
                    optimisticCompletedTimestamp={optimisticCompletedTimestamp}
                />
            )}
        </PromptWrapper>
    );
}
