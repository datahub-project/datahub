import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FormPrompt, SchemaField, SubmitFormPromptInput } from '../../../../../../types.generated';
import useStructuredPropertyPrompt from './useStructuredPropertyPrompt';
import CompletedPromptAuditStamp from './CompletedPromptAuditStamp';
import { applyOpacity } from '../../../../../shared/styleUtils';
import usePromptCompletionInfo from '../usePromptCompletionInfo';
import StructuredPropertyInput from '../../../components/styled/StructuredProperty/StructuredPropertyInput';

const PromptWrapper = styled.div<{ displayBulkStyles?: boolean }>`
    display: flex;
    justify-content: space-between;
    height: min-content;
    ${(props) => props.displayBulkStyles && `color: white;`}
`;

const PromptTitle = styled.div<{ displayBulkStyles?: boolean }>`
    font-size: 16px;
    font-weight: 600;
    line-height: 20px;
    ${(props) => props.displayBulkStyles && `font-size: 20px;`}
`;

const RequiredText = styled.span<{ displayBulkStyles?: boolean }>`
    font-size: 12px;
    margin-left: 4px;
    color: #a8071a;
    ${(props) =>
        props.displayBulkStyles &&
        `
        color: #FFCCC7;
        margin-left: 8px;
    `}
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
}

export default function StructuredPropertyPrompt({
    promptNumber,
    prompt,
    submitResponse,
    field,
    optimisticCompletedTimestamp,
}: Props) {
    const {
        hasEdited,
        selectedValues,
        selectSingleValue,
        toggleSelectedValue,
        submitStructuredPropertyResponse,
        updateSelectedValues,
    } = useStructuredPropertyPrompt({ prompt, submitResponse, field });
    const { isComplete, completedByName, completedByTime } = usePromptCompletionInfo({
        prompt,
        field,
        optimisticCompletedTimestamp,
    });

    const structuredProperty = prompt.structuredPropertyParams?.structuredProperty;
    if (!structuredProperty) return null;

    const { displayName, description } = structuredProperty.definition;
    const showSaveButton = hasEdited && selectedValues.length > 0;
    const showConfirmButton = !hasEdited && !isComplete && selectedValues.length > 0;

    return (
        <>
            <PromptWrapper>
                <PromptInputWrapper>
                    <PromptTitle>
                        {promptNumber !== undefined && <>{promptNumber}. </>}
                        {displayName}
                        {prompt.required && <RequiredText>required</RequiredText>}
                    </PromptTitle>
                    {description && <PromptSubTitle>{description}</PromptSubTitle>}
                    <InputSection>
                        <StructuredPropertyInput
                            structuredProperty={structuredProperty}
                            selectedValues={selectedValues}
                            selectSingleValue={selectSingleValue}
                            toggleSelectedValue={toggleSelectedValue}
                            updateSelectedValues={updateSelectedValues}
                        />
                    </InputSection>
                </PromptInputWrapper>
                {isComplete && !hasEdited && (
                    <CompletedPromptAuditStamp completedByName={completedByName} completedByTime={completedByTime} />
                )}
            </PromptWrapper>
            {(showSaveButton || showConfirmButton) && (
                <StyledButton type="primary" onClick={submitStructuredPropertyResponse}>
                    {showSaveButton ? 'Save' : 'Confirm'}
                </StyledButton>
            )}
        </>
    );
}
