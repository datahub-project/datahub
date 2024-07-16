import { Button } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import OwnershipTypesSelect from '@src/app/entityV2/shared/containers/profile/sidebar/Ownership/OwnershipTypesSelect';
import { useListOwnershipTypesQuery } from '@src/graphql/ownership.generated';
import {
    EntityType,
    FormPrompt,
    PromptCardinality,
    SchemaField,
    SubmitFormPromptInput,
} from '../../../../../../types.generated';
import CompletedPromptAuditStamp from '../CompletedPromptAuditStamp';
import { applyOpacity } from '../../../../../shared/styleUtils';
import { useEntityFormContext } from '../../EntityFormContext';
import BulkSubmissionButton from '../BulkSubmissionButton';
import usePromptCompletionInfo from '../usePromptCompletionInfo';
import { Editor } from '../../../tabs/Documentation/components/editor/Editor';
import { ColumnSelectorProps } from '../types';
import ColumnSelector from '../ColumnSelector';
import useOwnershipPrompt from './useOwnershipPrompt';
import UrnInput from '../StructuredPropertyPrompt/UrnInput/UrnInput';

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
    const { data: ownershipTypesData } = useListOwnershipTypesQuery({
        variables: {
            input: {},
        },
        fetchPolicy: 'cache-first',
    });
    const ownershipTypes = useMemo(() => {
        return ownershipTypesData?.listOwnershipTypes?.ownershipTypes || [];
    }, [ownershipTypesData]);

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

    const showSaveButton = !displayBulkPromptStyles && hasEdited && selectedValues.length > 0;
    const showConfirmButton = !displayBulkPromptStyles && !hasEdited && !isComplete && selectedValues.length > 0;

    return (
        <>
            <PromptWrapper displayBulkStyles={displayBulkPromptStyles}>
                <PromptInputWrapper>
                    <PromptTitle displayBulkStyles={displayBulkPromptStyles}>
                        {promptNumber !== undefined && <>{promptNumber}. </>}
                        {prompt.title}
                        {prompt.required && (
                            <RequiredText displayBulkStyles={displayBulkPromptStyles}>required</RequiredText>
                        )}
                    </PromptTitle>
                    {prompt.description && (
                        <PromptSubTitle>
                            <Editor content={prompt.description} readOnly editorStyle="padding: 0;" />
                        </PromptSubTitle>
                    )}
                    <InputSection>
                        <UrnInput
                            initialEntities={initialEntities}
                            allowedEntityTypes={[EntityType.CorpUser, EntityType.CorpGroup]}
                            isMultiple={prompt.ownershipParams?.cardinality === PromptCardinality.Multiple}
                            selectedValues={selectedValues}
                            updateSelectedValues={updateSelectedValues}
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
