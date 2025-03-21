import React, { useEffect, useState, useRef } from 'react';
import { IncidentStage, IncidentState, IncidentType } from '@src/types.generated';
import { Input } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { Editor } from '@src/alchemy-components/components/Editor/Editor';
import { useUserContext } from '@src/app/context/useUserContext';
import { Form } from 'antd';

import {
    INCIDENT_CATEGORIES,
    INCIDENT_OPTION_LABEL_MAPPING,
    INCIDENT_PRIORITIES,
    INCIDENT_STAGES,
    INCIDENT_STATES,
    IncidentAction,
} from '../constant';
import { getAssigneeWithURN, getLinkedAssetsData, validateForm } from '../utils';
import {
    IncidentFooter,
    SelectFormItem,
    SaveButton,
    StyledForm,
    StyledFormElements,
    InputFormItem,
    StyledSpinner,
} from './styledComponents';
import { IncidentEditorProps } from '../types';
import { IncidentLinkedAssetsList } from './IncidentLinkedAssetsList';
import { IncidentSelectField } from './IncidentSelectedField';
import { IncidentAssigneeSelector } from './IncidentAssigneeSelector';
import { useIncidentHandler } from './hooks/useIncidentHandler';

export const IncidentEditor = ({
    incidentUrn,
    onSubmit,
    data,
    onClose,
    mode = IncidentAction.CREATE,
}: IncidentEditorProps) => {
    const assigneeValues = data?.assignees && getAssigneeWithURN(data.assignees);
    const isFormValid = Boolean(
        data?.title?.length &&
            data?.description &&
            data?.type &&
            (data?.type !== IncidentType.Custom || data?.customType),
    );
    const { user } = useUserContext();
    const userHasChangedState = useRef(false);
    const isFirstRender = useRef(true);
    const [cachedAssignees, setCachedAssignees] = useState<any>([]);
    const [cachedLinkedAssets, setCachedLinkedAssets] = useState<any>([]);
    const [isLoadingAssigneeOrAssets, setIsLoadingAssigneeOrAssets] = useState(true);

    const [isRequiredFieldsFilled, setIsRequiredFieldsFilled] = useState<boolean>(
        mode === IncidentAction.EDIT ? isFormValid : false,
    );

    const { handleSubmit, form, isLoading } = useIncidentHandler({
        incidentUrn,
        mode,
        onSubmit,
        onClose,
        user,
        assignees: cachedAssignees,
        linkedAssets: cachedLinkedAssets,
    });
    const formValues = Form.useWatch([], form);

    useEffect(() => {
        // Set the initial value for the custom category field when it becomes visible
        if (formValues?.type === IncidentType.Custom) {
            if (form.getFieldValue('customType') === '') form.setFieldValue('customType', data?.customType || '');
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [formValues]);

    useEffect(() => {
        // Skip effect on first render
        if (isFirstRender.current) {
            isFirstRender.current = false;
            return;
        }

        // Ensure we don't override user's choice if they manually change the state
        if (
            mode === IncidentAction.EDIT &&
            (formValues?.status === IncidentStage.Fixed || formValues?.status === IncidentStage.NoActionRequired) &&
            formValues?.state !== IncidentState.Resolved
        ) {
            form.setFieldValue('state', IncidentState.Resolved);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [formValues?.status]);

    const handleValuesChange = (changedValues: any) => {
        Object.keys(changedValues).forEach((fieldName) => form.setFields([{ name: fieldName, errors: [] }]));
        // Update custom type status based on its value
        const changedFormValues = form.getFieldsValue();
        const { title, description, type, customType } = changedFormValues;

        // Mark that user manually changed the state
        if ('state' in changedValues) {
            userHasChangedState.current = true;
        }

        if (title?.trim() && description?.trim() && type && (type !== IncidentType.Custom || customType?.trim())) {
            setIsRequiredFieldsFilled(true); // Enable the button
        } else {
            setIsRequiredFieldsFilled(false); // Disable the button
        }
    };

    const actionButtonLabel = mode === IncidentAction.CREATE ? 'Create' : 'Update';
    const showCustomCategory = form.getFieldValue('type') === IncidentType.Custom;
    const isLinkedAssetPresent = !formValues?.resourceUrns?.length;
    const isSubmitButtonDisabled =
        !validateForm(form) ||
        !isRequiredFieldsFilled ||
        isLoadingAssigneeOrAssets ||
        isLinkedAssetPresent ||
        isLoading;

    return (
        <StyledForm
            form={form}
            layout="vertical"
            onFinish={handleSubmit}
            onValuesChange={handleValuesChange}
            initialValues={{
                title: data?.title || '',
                description: data?.description || '',
                type: data?.type || '',
                priority: data?.priority || '',
                status: data?.stage || '',
                customType: data?.customType || '',
                state: data?.state || '',
                message: data?.message || '',
            }}
        >
            <StyledFormElements data-testid="incident-editor-form-container">
                <InputFormItem label="Name" name="title">
                    <Input
                        label=""
                        placeholder="Provide a name..."
                        inputTestId="incident-name-input"
                        color={colors.gray[600]}
                    />
                </InputFormItem>
                <InputFormItem label="Description" name="description">
                    <Editor
                        doNotFocus
                        className="add-incident-description"
                        placeholder="Provide a description..."
                        content={mode === IncidentAction.EDIT ? data?.description : ''}
                    />
                </InputFormItem>
                <IncidentSelectField
                    incidentLabelMap={INCIDENT_OPTION_LABEL_MAPPING.category}
                    options={INCIDENT_CATEGORIES}
                    onUpdate={(value) => {
                        if (value !== IncidentType.Custom) {
                            form.setFieldValue('customType', '');
                        }
                    }}
                    form={form}
                    isDisabled={mode === IncidentAction.EDIT}
                    handleValuesChange={handleValuesChange}
                    value={formValues?.[INCIDENT_OPTION_LABEL_MAPPING.category.fieldName]}
                />
                {showCustomCategory && (
                    <SelectFormItem label="Custom Category" name="customType">
                        <Input
                            label=""
                            placeholder="Enter category name..."
                            required
                            styles={{
                                width: '50%',
                            }}
                            isDisabled={mode === IncidentAction.EDIT}
                            id="custom-incident-type-input"
                        />
                    </SelectFormItem>
                )}
                <IncidentSelectField
                    incidentLabelMap={INCIDENT_OPTION_LABEL_MAPPING.priority}
                    options={INCIDENT_PRIORITIES}
                    form={form}
                    handleValuesChange={handleValuesChange}
                    value={formValues?.[INCIDENT_OPTION_LABEL_MAPPING.priority.fieldName]}
                />
                <IncidentSelectField
                    incidentLabelMap={INCIDENT_OPTION_LABEL_MAPPING.stage}
                    options={INCIDENT_STAGES}
                    form={form}
                    handleValuesChange={handleValuesChange}
                    value={formValues?.[INCIDENT_OPTION_LABEL_MAPPING.stage.fieldName]}
                />
                <SelectFormItem label="Assignees" name="assigneeUrns" initialValue={assigneeValues || []}>
                    <IncidentAssigneeSelector form={form} data={data} setCachedAssignees={setCachedAssignees} />
                </SelectFormItem>
                <SelectFormItem
                    label="Linked Assets"
                    name="resourceUrns"
                    initialValue={getLinkedAssetsData(data?.linkedAssets) || []}
                >
                    <IncidentLinkedAssetsList
                        form={form}
                        data={data}
                        mode={mode}
                        setCachedLinkedAssets={setCachedLinkedAssets}
                        setIsLinkedAssetsLoading={setIsLoadingAssigneeOrAssets}
                    />
                </SelectFormItem>
                {mode === IncidentAction.EDIT && (
                    <IncidentSelectField
                        incidentLabelMap={INCIDENT_OPTION_LABEL_MAPPING.state}
                        options={INCIDENT_STATES}
                        form={form}
                        handleValuesChange={handleValuesChange}
                        showClear={false}
                        value={formValues?.[INCIDENT_OPTION_LABEL_MAPPING.state.fieldName]}
                    />
                )}
                {form.getFieldValue('state') === IncidentState.Resolved && (
                    <SelectFormItem
                        label="Resolution Note"
                        name="message"
                        rules={[{ required: false }]}
                        customStyle={{
                            color: colors.gray[600],
                        }}
                    >
                        <Input
                            label=""
                            placeholder="Add a resolution note......"
                            styles={{
                                width: '50%',
                            }}
                            id="incident-message"
                        />
                    </SelectFormItem>
                )}
            </StyledFormElements>
            <IncidentFooter>
                <SaveButton data-testid="incident-create-button" type="submit" disabled={isSubmitButtonDisabled}>
                    {/* {actionButtonLabel} */}
                    {isLoading ? (
                        <>
                            <StyledSpinner />
                            {actionButtonLabel === 'Create' ? 'Creating...' : 'Updating...'}
                        </>
                    ) : (
                        actionButtonLabel
                    )}
                </SaveButton>
            </IncidentFooter>
        </StyledForm>
    );
};
