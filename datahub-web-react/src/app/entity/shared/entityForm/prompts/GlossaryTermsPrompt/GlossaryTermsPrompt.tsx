import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import BulkSubmissionButton from '@app/entity/shared/entityForm/prompts/BulkSubmissionButton';
import ColumnSelector from '@app/entity/shared/entityForm/prompts/ColumnSelector';
import CompletedPromptAuditStamp from '@app/entity/shared/entityForm/prompts/CompletedPromptAuditStamp';
import useGlossaryTermsPrompt from '@app/entity/shared/entityForm/prompts/GlossaryTermsPrompt/useGlossaryTermsPrompt';
import PromptHeader from '@app/entity/shared/entityForm/prompts/PromptHeader';
import UrnInput from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/UrnInput/UrnInput';
import { ColumnSelectorProps } from '@app/entity/shared/entityForm/prompts/types';
import usePromptCompletionInfo from '@app/entity/shared/entityForm/prompts/usePromptCompletionInfo';
import { applyOpacity } from '@app/shared/styleUtils';

import { EntityType, FormPrompt, PromptCardinality, SchemaField, SubmitFormPromptInput } from '@types';

const PromptWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    height: min-content;
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

export default function GlossaryTermsPrompt({
    promptNumber,
    prompt,
    submitResponse,
    field,
    optimisticCompletedTimestamp,
    columnSelectorProps,
}: Props) {
    const { hasEdited, selectedValues, initialEntities, submitGlossaryTermsResponse, updateSelectedValues } =
        useGlossaryTermsPrompt({
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

    const showSaveButton = !displayBulkPromptStyles && hasEdited && selectedValues.length > 0;
    const showConfirmButton = !displayBulkPromptStyles && !hasEdited && !isComplete && selectedValues.length > 0;
    const allowedTerms = prompt.glossaryTermsParams?.resolvedAllowedTerms || [];

    return (
        <>
            <PromptWrapper>
                <PromptInputWrapper>
                    <PromptHeader
                        title={prompt.title}
                        description={prompt.description}
                        promptNumber={promptNumber}
                        required={prompt.required}
                    />
                    <InputSection>
                        <UrnInput
                            initialEntities={initialEntities}
                            allowedEntities={prompt.glossaryTermsParams?.resolvedAllowedTerms || []}
                            allowedEntityTypes={[EntityType.GlossaryTerm]}
                            isMultiple={prompt.glossaryTermsParams?.cardinality === PromptCardinality.Multiple}
                            selectedValues={selectedValues}
                            updateSelectedValues={updateSelectedValues}
                            promptType={prompt.type}
                            placeholder={allowedTerms.length ? 'Select from the provided Glossary Terms...' : undefined}
                        />
                        {displayBulkPromptStyles && (
                            <BulkSubmissionButton
                                isDisabled={!selectedValues.length || !selectedEntities.length}
                                submitResponse={submitGlossaryTermsResponse}
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
                <StyledButton type="primary" onClick={submitGlossaryTermsResponse}>
                    {showSaveButton ? 'Save' : 'Confirm'}
                </StyledButton>
            )}
        </>
    );
}
