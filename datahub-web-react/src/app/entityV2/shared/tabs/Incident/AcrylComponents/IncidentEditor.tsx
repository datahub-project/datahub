import { Form } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { IncidentAssigneeSelector } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentAssigneeSelector';
import { IncidentLinkedAssetsList } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentLinkedAssetsList';
import { IncidentSelectField } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentSelectedField';
import { useIncidentHandler } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/hooks/useIncidentHandler';
import {
    IncidentFooter,
    InputFormItem,
    SaveButton,
    SelectFormItem,
    StyledForm,
    StyledFormElements,
    StyledSpinner,
} from '@app/entityV2/shared/tabs/Incident/AcrylComponents/styledComponents';
import {
    INCIDENT_CATEGORIES,
    INCIDENT_OPTION_LABEL_MAPPING,
    INCIDENT_PRIORITIES,
    INCIDENT_STAGES,
    INCIDENT_STATES,
    IncidentAction,
} from '@app/entityV2/shared/tabs/Incident/constant';
import { IncidentEditorProps } from '@app/entityV2/shared/tabs/Incident/types';
import { getAssigneeWithURN, getLinkedAssetsData, validateForm } from '@app/entityV2/shared/tabs/Incident/utils';
import { Input } from '@src/alchemy-components';
import { Editor } from '@src/alchemy-components/components/Editor/Editor';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { useUserContext } from '@src/app/context/useUserContext';
import { IncidentStage, IncidentState, IncidentType } from '@src/types.generated';

const HalfWidthInput = styled(Input)`
    width: 50%;
`;

export const IncidentEditor = ({
    entity,
    incidentUrn,
    onSubmit,
    data,
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
    const [cachedAssignees, setCachedAssignees] = useState<any[]>([]);
    const [cachedLinkedAssets, setCachedLinkedAssets] = useState<any[]>([]);
    const [isLoadingAssigneeOrAssets, setIsLoadingAssigneeOrAssets] = useState(true);

    const [isRequiredFieldsFilled, setIsRequiredFieldsFilled] = useState<boolean>(
        mode === IncidentAction.EDIT ? isFormValid : false,
    );

    const { handleSubmit, form, isLoading } = useIncidentHandler({
        incidentUrn,
        mode,
        onSubmit,
        user,
        assignees: cachedAssignees,
        linkedAssets: cachedLinkedAssets,
        entity,
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

    const showCustomCategory = form.getFieldValue('type') === IncidentType.Custom;
    const isLinkedAssetMissing = !formValues?.resourceUrns?.length;
    const isSubmitButtonDisabled =
        !validateForm(form) ||
        !isRequiredFieldsFilled ||
        isLoadingAssigneeOrAssets ||
        isLinkedAssetMissing ||
        isLoading;

    const actionButtonLabel = mode === IncidentAction.CREATE ? 'Create' : 'Update';
    const actionButton = isLoading ? (
        <>
            <StyledSpinner />
            {actionButtonLabel === 'Create' ? 'Creating...' : 'Updating...'}
        </>
    ) : (
        actionButtonLabel
    );

    const resolutionInput = form.getFieldValue('state') === IncidentState.Resolved && (
        <SelectFormItem
            label="Resolution Note"
            name="message"
            rules={[{ required: false }]}
            customStyle={{
                color: colors.gray[600],
            }}
        >
            <HalfWidthInput label="" placeholder="Add a resolution note......" id="incident-message" />
        </SelectFormItem>
    );

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
                        <HalfWidthInput
                            label=""
                            placeholder="Enter category name..."
                            required
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
                        initialUrn={entity?.urn}
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
                {resolutionInput}
            </StyledFormElements>
            <IncidentFooter>
                <SaveButton data-testid="incident-create-button" type="submit" disabled={isSubmitButtonDisabled}>
                    {actionButton}
                </SaveButton>
            </IncidentFooter>
        </StyledForm>
    );
};
