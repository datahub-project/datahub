import { message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useMutationUrn } from '@app/entity/shared/EntityContext';
import StructuredPropertyPrompt from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/StructuredPropertyPrompt';

import { useSubmitFormPromptMutation } from '@graphql/form.generated';
import { FormPromptType, FormPrompt as PromptEntity, SchemaField, SubmitFormPromptInput } from '@types';

export const PromptWrapper = styled.div`
    background-color: white;
    border-radius: 8px;
    padding: 24px;
    margin-bottom: 8px;
`;

interface Props {
    promptNumber?: number;
    prompt: PromptEntity;
    field?: SchemaField;
    associatedUrn?: string;
}

export default function Prompt({ promptNumber, prompt, field, associatedUrn }: Props) {
    const [optimisticCompletedTimestamp, setOptimisticCompletedTimestamp] = useState<number | null>(null);
    const urn = useMutationUrn();
    const [submitFormPrompt] = useSubmitFormPromptMutation();

    function submitResponse(input: SubmitFormPromptInput, onSuccess: () => void) {
        submitFormPrompt({ variables: { urn: associatedUrn || urn, input } })
            .then(() => {
                onSuccess();
                setOptimisticCompletedTimestamp(Date.now());
            })
            .catch(() => {
                message.error('Unknown error while submitting form response');
            });
    }

    return (
        <PromptWrapper>
            {prompt.type === FormPromptType.StructuredProperty && (
                <StructuredPropertyPrompt
                    promptNumber={promptNumber}
                    prompt={prompt}
                    submitResponse={submitResponse}
                    optimisticCompletedTimestamp={optimisticCompletedTimestamp}
                />
            )}
            {prompt.type === FormPromptType.FieldsStructuredProperty && (
                <StructuredPropertyPrompt
                    promptNumber={promptNumber}
                    prompt={prompt}
                    submitResponse={submitResponse}
                    field={field}
                    optimisticCompletedTimestamp={optimisticCompletedTimestamp}
                />
            )}
        </PromptWrapper>
    );
}
