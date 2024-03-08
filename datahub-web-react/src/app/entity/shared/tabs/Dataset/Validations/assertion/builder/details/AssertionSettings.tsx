import React, { useState } from 'react';

import { Form } from 'antd';

import { Assertion, AssertionType, Monitor, Entity } from '../../../../../../../../../types.generated';
import { EditButton } from './EditButton';
import { AssertionSettingsHeader } from './AssertionSettingsHeader';
import { AssertionMonitorBuilderState } from '../types';
import { createAssertionMonitorBuilderState } from '../utils';
import { SaveButton } from './SaveButton';
import { useUpsertAssertionMonitor } from '../useUpsertAssertionMonitor';
import { SqlAssertionBuilder } from '../steps/sql/SqlAssertionBuilder';
import { DatasetFreshnessAssertionBuilder } from '../steps/freshness/DatasetFreshnessAssertionBuilder';
import { VolumeAssertionBuilder } from '../steps/volume/VolumeAssertionBuilder';
import { FieldAssertionBuilder } from '../steps/field/FieldAssertionBuilder';

type Props = {
    assertion: Assertion;
    entity: Entity;
    monitor?: Monitor;
    editable?: boolean;
    editAllowed?: boolean;
    refetch?: () => void;
};

export const AssertionSettings = (props: Props) => {
    const initialState = createAssertionMonitorBuilderState(props.assertion, props.entity, props.monitor);
    const [builderState, setBuilderState] = useState<AssertionMonitorBuilderState>(initialState);
    const [editing, setEditing] = useState<boolean>(false);
    const [form] = Form.useForm();

    const updateAssertionMonitor = useUpsertAssertionMonitor(
        builderState,
        () => {
            props.refetch?.();
        },
        true,
    );

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
        setEditing(false);
        await updateAssertionMonitor();
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
                        (!editing && (
                            <EditButton
                                disabled={!props.editAllowed}
                                tooltip={finalTooltip}
                                onClick={() => {
                                    if (props.editAllowed) setEditing(true);
                                }}
                            />
                        )) || <SaveButton tooltip="Save changes to this assertion" onClick={save} />
                    }
                    onChangeDescription={updateDescription}
                    editing={editing}
                />
            )}
            <Form initialValues={builderState} form={form}>
                {props.assertion.info?.type === AssertionType.Sql && (
                    <SqlAssertionBuilder state={builderState} updateState={setBuilderState} editing={editing} />
                )}
                {props.assertion.info?.type === AssertionType.Freshness && (
                    <DatasetFreshnessAssertionBuilder
                        state={builderState}
                        updateState={setBuilderState}
                        editing={editing}
                    />
                )}
                {props.assertion.info?.type === AssertionType.Volume && (
                    <VolumeAssertionBuilder state={builderState} updateState={setBuilderState} editing={editing} />
                )}
                {props.assertion.info?.type === AssertionType.Field && (
                    <FieldAssertionBuilder state={builderState} updateState={setBuilderState} editing={editing} />
                )}
            </Form>
        </>
    );
};
