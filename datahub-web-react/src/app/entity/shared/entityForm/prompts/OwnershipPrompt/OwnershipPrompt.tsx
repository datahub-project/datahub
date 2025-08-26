import { Button } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import BulkSubmissionButton from '@app/entity/shared/entityForm/prompts/BulkSubmissionButton';
import ColumnSelector from '@app/entity/shared/entityForm/prompts/ColumnSelector';
import CompletedPromptAuditStamp from '@app/entity/shared/entityForm/prompts/CompletedPromptAuditStamp';
import useOwnershipPrompt from '@app/entity/shared/entityForm/prompts/OwnershipPrompt/useOwnershipPrompt';
import PromptHeader from '@app/entity/shared/entityForm/prompts/PromptHeader';
import UrnInput from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/UrnInput/UrnInput';
import { ColumnSelectorProps } from '@app/entity/shared/entityForm/prompts/types';
import usePromptCompletionInfo from '@app/entity/shared/entityForm/prompts/usePromptCompletionInfo';
import { applyOpacity } from '@app/shared/styleUtils';
import OwnershipTypesSelect from '@src/app/entityV2/shared/containers/profile/sidebar/Ownership/OwnershipTypesSelect';
import { useListOwnershipTypesQuery } from '@src/graphql/ownership.generated';

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
    field?: SchemaField;
    optimisticCompletedTimestamp?: number | null;
    columnSelectorProps?: ColumnSelectorProps;
}

export default function OwnershipPrompt({
    promptNumber,
    prompt,
    submitResponse,
    field,
    optimisticCompletedTimestamp,
    columnSelectorProps,
}: Props) {
    const allowedOwnershipTypes = useMemo(
        () => prompt.ownershipParams?.allowedOwnershipTypes || [],
        [prompt.ownershipParams?.allowedOwnershipTypes],
    );
    const { data: ownershipTypesData } = useListOwnershipTypesQuery({
        variables: {
            input: {},
        },
        skip: allowedOwnershipTypes.length > 0,
        fetchPolicy: 'cache-first',
    });
    const ownershipTypes = useMemo(() => {
        return allowedOwnershipTypes.length > 0
            ? allowedOwnershipTypes
            : ownershipTypesData?.listOwnershipTypes?.ownershipTypes || [];
    }, [ownershipTypesData, allowedOwnershipTypes]);

    const {
        hasEdited,
        selectedValues,
        selectedOwnerTypeUrn,
        initialEntities,
        updateSelectedOwnerTypeUrn,
        submitOwnershipResponse,
        updateSelectedValues,
    } = useOwnershipPrompt({
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

    const showSaveButton = !displayBulkPromptStyles && hasEdited && selectedValues.length > 0 && selectedOwnerTypeUrn;
    const showConfirmButton =
        !displayBulkPromptStyles && !hasEdited && !isComplete && selectedValues.length > 0 && selectedOwnerTypeUrn;
    const allowedOwners = prompt.ownershipParams?.allowedOwners || [];

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
                            allowedEntities={allowedOwners}
                            allowedEntityTypes={[EntityType.CorpUser, EntityType.CorpGroup]}
                            isMultiple={prompt.ownershipParams?.cardinality === PromptCardinality.Multiple}
                            selectedValues={selectedValues}
                            updateSelectedValues={updateSelectedValues}
                            placeholder={
                                allowedOwners.length ? 'Select from the provided Users and Groups...' : undefined
                            }
                        />
                        <OwnershipTypesSelect
                            selectedOwnerTypeUrn={selectedOwnerTypeUrn}
                            ownershipTypes={ownershipTypes}
                            onSelectOwnerType={updateSelectedOwnerTypeUrn}
                        />
                        {displayBulkPromptStyles && (
                            <BulkSubmissionButton
                                isDisabled={!selectedValues.length || !selectedEntities.length}
                                submitResponse={submitOwnershipResponse}
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
                <StyledButton type="primary" onClick={submitOwnershipResponse}>
                    {showSaveButton ? 'Save' : 'Confirm'}
                </StyledButton>
            )}
        </>
    );
}
