import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import RichTextInput from '@app/entity/shared/components/styled/StructuredProperty/RichTextInput';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import BulkSubmissionButton from '@app/entity/shared/entityForm/prompts/BulkSubmissionButton';
import ColumnSelector from '@app/entity/shared/entityForm/prompts/ColumnSelector';
import CompletedPromptAuditStamp from '@app/entity/shared/entityForm/prompts/CompletedPromptAuditStamp';
import useDocumentationPrompt from '@app/entity/shared/entityForm/prompts/DocumentationPrompt/useDocumentationPrompt';
import PromptHeader from '@app/entity/shared/entityForm/prompts/PromptHeader';
import { ColumnSelectorProps } from '@app/entity/shared/entityForm/prompts/types';
import usePromptCompletionInfo from '@app/entity/shared/entityForm/prompts/usePromptCompletionInfo';
import { applyOpacity } from '@app/shared/styleUtils';

import { FormPrompt, SchemaField, SubmitFormPromptInput } from '@types';

const PromptWrapper = styled.div<{ displayBulkStyles?: boolean }>`
    display: flex;
    justify-content: space-between;
    height: min-content;
    ${(props) => props.displayBulkStyles && `color: white;`}
`;

export const PromptSubTitle = styled.div`
    font-size: 14px;
    font-weight: 500;
    line-height: 18px;
    margin-top: 4px;
`;

const InputSection = styled.div`
    margin-top: 8px;
    display: flex;
`;

const StyledButton = styled(Button)`
    margin-top: 16px;

    &:focus {
        box-shadow: 0 0 3px 2px ${(props) => applyOpacity(props.theme.styles['primary-color'] || '', 50)};
    }
`;

const PromptInputWrapper = styled.div`
    flex: 1;
    margin-right: 8px;
`;

interface Props {
    promptNumber?: number;
    prompt: FormPrompt;
    submitResponse: (input: SubmitFormPromptInput, onSuccess: () => void) => void;
    field?: SchemaField;
    optimisticCompletedTimestamp?: number | null;
    columnSelectorProps?: ColumnSelectorProps;
}

export default function DocumentationPrompt({
    promptNumber,
    prompt,
    submitResponse,
    field,
    optimisticCompletedTimestamp,
    columnSelectorProps,
}: Props) {
    const { hasEdited, documentationValue, updateDocumentation, submitDocumentationResponse } = useDocumentationPrompt({
        prompt,
        submitResponse,
        field,
    });

    const {
        prompt: { displayBulkPromptStyles },
        entity: { selectedEntities },
    } = useEntityFormContext();

    const { isComplete, completedByName, completedByTime } = usePromptCompletionInfo({
        prompt,
        field,
        optimisticCompletedTimestamp,
    });

    const showSaveButton = !displayBulkPromptStyles && hasEdited && !!documentationValue;
    const showConfirmButton = !displayBulkPromptStyles && !hasEdited && !isComplete && !!documentationValue;

    return (
        <>
            <PromptWrapper displayBulkStyles={displayBulkPromptStyles}>
                <PromptInputWrapper>
                    <PromptHeader
                        title={prompt.title}
                        description={prompt.description}
                        promptNumber={promptNumber}
                        required={prompt.required}
                    />
                    <InputSection>
                        <RichTextInput
                            selectedValues={[documentationValue]}
                            updateSelectedValues={(v) => updateDocumentation(v.length ? (v[0] as string) : '')}
                        />
                        {displayBulkPromptStyles && (
                            <BulkSubmissionButton
                                isDisabled={!documentationValue || !selectedEntities.length}
                                submitResponse={submitDocumentationResponse}
                            />
                        )}
                    </InputSection>
                    {field && columnSelectorProps && (showSaveButton || showConfirmButton) && (
                        <ColumnSelector field={field} {...columnSelectorProps} />
                    )}
                </PromptInputWrapper>
                {isComplete && !hasEdited && !displayBulkPromptStyles && (
                    <CompletedPromptAuditStamp completedByName={completedByName} completedByTime={completedByTime} />
                )}
            </PromptWrapper>
            {(showSaveButton || showConfirmButton) && (
                <StyledButton type="primary" onClick={submitDocumentationResponse}>
                    {showSaveButton ? 'Save' : 'Confirm'}
                </StyledButton>
            )}
        </>
    );
}
