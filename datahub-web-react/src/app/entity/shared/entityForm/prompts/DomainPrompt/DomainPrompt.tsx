import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { EntityType, FormPrompt, SubmitFormPromptInput } from '../../../../../../types.generated';
import CompletedPromptAuditStamp from '../CompletedPromptAuditStamp';
import { applyOpacity } from '../../../../../shared/styleUtils';
import { useEntityFormContext } from '../../EntityFormContext';
import BulkSubmissionButton from '../BulkSubmissionButton';
import usePromptCompletionInfo from '../usePromptCompletionInfo';
import UrnInput from '../StructuredPropertyPrompt/UrnInput/UrnInput';
import PromptHeader from '../PromptHeader';
import useDomainPrompt from './useDomainPrompt';

const PromptWrapper = styled.div<{ displayBulkStyles?: boolean }>`
    display: flex;
    justify-content: space-between;
    height: min-content;
    ${(props) => props.displayBulkStyles && `color: white;`}
`;

const InputSection = styled.div`
    margin-top: 8px;
    display: flex;
    gap: 8px;
    .ant-select-selector {
        min-height: 40px;
    }
    .ant-select-single {
        width: 40%;
        max-width: 300px;
        max-height: 40px;
        .ant-select-selector {
            padding-top: 3px;
            height: 100%;
            font-size: 14px;
        }
    }
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
    optimisticCompletedTimestamp?: number | null;
}

export default function DomainPrompt({ promptNumber, prompt, submitResponse, optimisticCompletedTimestamp }: Props) {
    const { hasEdited, selectedDomain, initialEntity, submitDomainResponse, updateSelectedDomain } = useDomainPrompt({
        prompt,
        submitResponse,
    });

    const {
        prompt: { displayBulkPromptStyles },
        entity: { selectedEntities },
    } = useEntityFormContext();

    const { isComplete, completedByName, completedByTime } = usePromptCompletionInfo({
        prompt,
        field: undefined,
        optimisticCompletedTimestamp,
    });

    const showSaveButton = !displayBulkPromptStyles && hasEdited && selectedDomain;
    const showConfirmButton = !displayBulkPromptStyles && !hasEdited && !isComplete && selectedDomain;
    const allowedDomains = prompt.domainParams?.allowedDomains || [];

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
                        <UrnInput
                            initialEntities={initialEntity ? [initialEntity] : []}
                            allowedEntities={allowedDomains}
                            allowedEntityTypes={[EntityType.Domain]}
                            isMultiple={false}
                            selectedValues={[selectedDomain]}
                            updateSelectedValues={(values) => updateSelectedDomain(values.length ? values[0] : null)}
                            placeholder={allowedDomains.length ? 'Select from the provided Domains...' : undefined}
                        />
                        {displayBulkPromptStyles && (
                            <BulkSubmissionButton
                                isDisabled={!selectedDomain || !selectedEntities.length}
                                submitResponse={submitDomainResponse}
                            />
                        )}
                    </InputSection>
                </PromptInputWrapper>
                {isComplete && !hasEdited && !displayBulkPromptStyles && (
                    <CompletedPromptAuditStamp completedByName={completedByName} completedByTime={completedByTime} />
                )}
            </PromptWrapper>
            {(showSaveButton || showConfirmButton) && (
                <StyledButton type="primary" onClick={submitDomainResponse}>
                    {showSaveButton ? 'Save' : 'Confirm'}
                </StyledButton>
            )}
        </>
    );
}
