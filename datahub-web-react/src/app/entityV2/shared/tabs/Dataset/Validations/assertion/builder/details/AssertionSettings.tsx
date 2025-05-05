import { Button, Tooltip } from '@components';
import { Form } from 'antd';
import React, { useRef, useState } from 'react';
import styled from 'styled-components';

import { useConnectionWithRunAssertionCapabilitiesForEntityExists } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionSettingsHeader } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/AssertionSettingsHeader';
import { EditButton } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/EditButton';
import { SaveButton } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/SaveButton';
import { AssertionActionsSection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/actions/AssertionActionsSection';
import { FieldAssertionBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/FieldAssertionBuilder';
import { DatasetFreshnessAssertionBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/freshness/DatasetFreshnessAssertionBuilder';
import { VolumeInferenceAdjusterHandle } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/VolumeInferenceAdjuster';
import { TestAssertionModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/preview/TestAssertionModal';
import { SchemaAssertionBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/schema/SchemaAssertionBuilder';
import { SqlAssertionBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlAssertionBuilder';
import { useTestAssertionModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/utils';
import { VolumeAssertionBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/VolumeAssertionBuilder';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { useUpdateAssertionMetadataWithBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/useUpdateAssertionMetadata';
import { useUpsertAssertionMonitor } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/useUpsertAssertionMonitor';
import {
    createAssertionMonitorBuilderState,
    getAssertionInput,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/utils';
import {
    AssertionEditabilityScopeType,
    getAssertionEditabilityType,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/assertionUtils';

import { Assertion, AssertionSourceType, AssertionType, Entity, Monitor } from '@types';

type Props = {
    assertion: Assertion;
    entity: Entity;
    monitor?: Monitor;
    editable?: boolean;
    editAllowed?: boolean;
    refetch?: () => void;
};

const StyledButton = styled(Button)`
    margin-right: 10px;
`;

export const AssertionContainer = styled.div`
    display: flex;
    justify-content: space-around;
`;

export const AssertionSettings = (props: Props) => {
    const initialState = createAssertionMonitorBuilderState(props.assertion, props.entity, props.monitor);
    const [builderState, setBuilderState] = useState<AssertionMonitorBuilderState>(initialState);
    const [editing, setEditing] = useState<boolean>(false);
    const [form] = Form.useForm();
    const inferenceAdjusterRef = useRef<VolumeInferenceAdjusterHandle>(null);

    const isAiInferred = props.assertion.info?.source?.type === AssertionSourceType.Inferred;

    const editabilityType = getAssertionEditabilityType(props.assertion);

    const isFullEditingDisabled = !(editing && editabilityType === AssertionEditabilityScopeType.FULL);
    const isDescriptionEditingDisabled = !(
        editing &&
        (editabilityType === AssertionEditabilityScopeType.FULL ||
            editabilityType === AssertionEditabilityScopeType.ACTIONS_AND_DESCRIPTION)
    );
    const isActionsEditingDisabled = !(editing && editabilityType !== AssertionEditabilityScopeType.NONE);

    const updateAssertionMonitor = useUpsertAssertionMonitor(
        builderState,
        () => {
            props.refetch?.();
        },
        true,
    );
    const updateAssertionMetadata = useUpdateAssertionMetadataWithBuilderState(builderState, () => {
        props.refetch?.();
    });

    const { isTestAssertionModalVisible, hideTestAssertionModal, showTestAssertionModal } = useTestAssertionModal();

    const isTestAssertionActionDisabled = !useConnectionWithRunAssertionCapabilitiesForEntityExists(
        props.entity.urn ?? '',
    );

    const tryTestAssertion = async () => {
        try {
            await form.validateFields();
            showTestAssertionModal();
        } catch {
            // Ignore validation errors
        }
    };

    const validateForm = async () => {
        try {
            await form.validateFields();
            return true;
        } catch (e) {
            console.warn('Validate Failed:', e);
            return false;
        }
    };

    const save = async () => {
        const isValid = await validateForm();
        if (!isValid) return;
        if (editabilityType === AssertionEditabilityScopeType.NONE) return;
        setEditing(false);
        if (isAiInferred && inferenceAdjusterRef.current) {
            inferenceAdjusterRef.current.triggerRegeneration();
        }
        if (editabilityType === AssertionEditabilityScopeType.FULL) {
            await updateAssertionMonitor();
        } else {
            await updateAssertionMetadata();
        }
    };

    const updateDescription = (newValue: string) => {
        setBuilderState({
            ...builderState,
            assertion: {
                ...builderState.assertion,
                description: newValue,
            },
        });
    };

    const editButtonTooltip = props.editable ? 'Edit the assertion settings' : undefined;
    const authorizedToEditTooltip = !props.editAllowed ? 'You are not authorized to edit!' : undefined;
    // If the user is not authorized to edit, we show the tooltip for that, otherwise we show the edit button tooltip
    const finalTooltip = props.editAllowed ? editButtonTooltip : authorizedToEditTooltip;

    return (
        <>
            {props.editable && (
                <AssertionSettingsHeader
                    description={builderState.assertion?.description || undefined}
                    showDivider={false}
                    action={
                        !editing ? (
                            <EditButton
                                disabled={!props.editAllowed}
                                tooltip={finalTooltip}
                                onClick={() => {
                                    if (props.editAllowed) setEditing(true);
                                }}
                            />
                        ) : (
                            <>
                                <AssertionContainer>
                                    {!isAiInferred && (
                                        <Tooltip
                                            title={
                                                isTestAssertionActionDisabled
                                                    ? 'Trying assertions is not supported for sources with remote executors.'
                                                    : 'Try this assertion out!'
                                            }
                                        >
                                            <StyledButton
                                                variant="outline"
                                                onClick={tryTestAssertion}
                                                disabled={isTestAssertionActionDisabled}
                                            >
                                                Try it out
                                            </StyledButton>
                                        </Tooltip>
                                    )}
                                    <SaveButton tooltip="Save changes to this assertion" onClick={save} />
                                </AssertionContainer>

                                <TestAssertionModal
                                    visible={isTestAssertionModalVisible}
                                    handleClose={hideTestAssertionModal}
                                    input={getAssertionInput(builderState, props?.entity?.urn)}
                                />
                            </>
                        )
                    }
                    onChangeDescription={updateDescription}
                    descriptionDisabled={isDescriptionEditingDisabled}
                />
            )}
            <Form initialValues={builderState} form={form}>
                {props.assertion.info?.type === AssertionType.Sql ? (
                    <SqlAssertionBuilder
                        state={builderState}
                        updateState={setBuilderState}
                        disabled={isFullEditingDisabled}
                    />
                ) : null}
                {props.assertion.info?.type === AssertionType.Freshness ? (
                    <DatasetFreshnessAssertionBuilder
                        state={builderState}
                        updateState={setBuilderState}
                        disabled={isFullEditingDisabled}
                        isEditMode
                    />
                ) : null}
                {props.assertion.info?.type === AssertionType.Volume ? (
                    <VolumeAssertionBuilder
                        state={builderState}
                        updateState={setBuilderState}
                        disabled={isFullEditingDisabled}
                        isEditMode
                        onSave={save}
                        inferenceAdjusterRef={inferenceAdjusterRef}
                    />
                ) : null}
                {props.assertion.info?.type === AssertionType.Field ? (
                    <FieldAssertionBuilder
                        state={builderState}
                        updateState={setBuilderState}
                        disabled={isFullEditingDisabled}
                        isEditMode
                    />
                ) : null}
                {props.assertion.info?.type === AssertionType.DataSchema ? (
                    <SchemaAssertionBuilder
                        state={builderState}
                        updateState={setBuilderState}
                        disabled={isFullEditingDisabled}
                    />
                ) : null}
                <AssertionActionsSection
                    state={builderState}
                    updateState={setBuilderState}
                    disabled={isActionsEditingDisabled}
                />
            </Form>
        </>
    );
};
