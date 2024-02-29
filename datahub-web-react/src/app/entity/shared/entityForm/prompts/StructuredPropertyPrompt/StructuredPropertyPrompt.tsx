import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import {
    FormPrompt,
    PropertyCardinality,
    SchemaField,
    StdDataType,
    SubmitFormPromptInput,
} from '../../../../../../types.generated';
import SingleSelectInput from './SingleSelectInput';
import MultiSelectInput from './MultiSelectInput';
import useStructuredPropertyPrompt from './useStructuredPropertyPrompt';
import StringInput from './StringInput';
import RichTextInput from './RichTextInput';
import DateInput from './DateInput';
import NumberInput from './NumberInput';
import UrnInput from './UrnInput/UrnInput';
import CompletedPromptAuditStamp from './CompletedPromptAuditStamp';
import { applyOpacity } from '../../../../../shared/styleUtils';
import usePromptCompletionInfo from '../usePromptCompletionInfo';

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
        hasEditedPrompt,
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

    const { displayName, description, allowedValues, cardinality, valueType } = structuredProperty.definition;
    const showSaveButton = hasEditedPrompt && selectedValues.length > 0;
    const showConfirmButton = !hasEditedPrompt && !isComplete && selectedValues.length > 0;

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
                        {allowedValues && allowedValues.length > 0 && (
                            <>
                                {cardinality === PropertyCardinality.Single && (
                                    <SingleSelectInput
                                        allowedValues={allowedValues}
                                        selectedValues={selectedValues}
                                        selectSingleValue={selectSingleValue}
                                    />
                                )}
                                {cardinality === PropertyCardinality.Multiple && (
                                    <MultiSelectInput
                                        allowedValues={allowedValues}
                                        selectedValues={selectedValues}
                                        toggleSelectedValue={toggleSelectedValue}
                                        updateSelectedValues={updateSelectedValues}
                                    />
                                )}
                            </>
                        )}
                        {!allowedValues && valueType.info.type === StdDataType.String && (
                            <StringInput
                                selectedValues={selectedValues}
                                cardinality={cardinality}
                                updateSelectedValues={updateSelectedValues}
                            />
                        )}
                        {!allowedValues && valueType.info.type === StdDataType.RichText && (
                            <RichTextInput
                                selectedValues={selectedValues}
                                updateSelectedValues={updateSelectedValues}
                            />
                        )}
                        {!allowedValues && valueType.info.type === StdDataType.Date && (
                            <DateInput selectedValues={selectedValues} updateSelectedValues={updateSelectedValues} />
                        )}
                        {!allowedValues && valueType.info.type === StdDataType.Number && (
                            <NumberInput selectedValues={selectedValues} updateSelectedValues={updateSelectedValues} />
                        )}
                        {!allowedValues && valueType.info.type === StdDataType.Urn && (
                            <UrnInput
                                structuredProperty={structuredProperty}
                                selectedValues={selectedValues}
                                updateSelectedValues={updateSelectedValues}
                            />
                        )}
                    </InputSection>
                </PromptInputWrapper>
                {isComplete && !hasEditedPrompt && (
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
