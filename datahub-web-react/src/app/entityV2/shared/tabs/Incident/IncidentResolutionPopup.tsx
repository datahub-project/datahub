import React from 'react';
import { Modal, Form, Input, message } from 'antd';
import { IncidentStage, IncidentState } from '@src/types.generated';
import { Button, colors } from '@src/alchemy-components';
import { useUpdateIncidentStatusMutation } from '@src/graphql/mutations.generated';
import { useApolloClient } from '@apollo/client';
import { useUserContext } from '@src/app/context/useUserContext';

import handleGraphQLError from '@src/app/shared/handleGraphQLError';
import analytics, { EntityActionType, EventType } from '@src/app/analytics';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import { IncidentTableRow } from './types';
import { IncidentSelectField } from './AcrylComponents/IncidentSelectedField';
import { INCIDENT_OPTION_LABEL_MAPPING, INCIDENT_RESOLUTION_STAGES } from './constant';
import { FormItem, ModalHeading, ModalTitleContainer } from './styledComponents';
import { getCacheIncident } from './AcrylComponents/hooks/useIncidentHandler';
import { PAGE_SIZE, updateActiveIncidentInCache } from './incidentUtils';

type IncidentResolutionPopupProps = {
    incident: IncidentTableRow;
    handleClose: () => void;
};

const { TextArea } = Input;

const modalBodyStyle = { fontFamily: 'Mulish, sans-serif' };

const ModalTitle = () => (
    <ModalTitleContainer>
        <ModalHeading>Resolve Incident</ModalHeading>
    </ModalTitleContainer>
);

export const IncidentResolutionPopup = ({ incident, handleClose }: IncidentResolutionPopupProps) => {
    const client = useApolloClient();
    const { user } = useUserContext();
    const { urn, entityType } = useEntityData();
    const [updateIncidentStatusMutation] = useUpdateIncidentStatusMutation();
    const [form] = Form.useForm();

    const handleValuesChange = (changedValues: any) => {
        Object.keys(changedValues).forEach((fieldName) => form.setFields([{ name: fieldName, errors: [] }]));
    };

    const handleResolveIncident = (formData: any) => {
        message.loading({ content: 'Updating...' });

        updateIncidentStatusMutation({
            variables: {
                urn: incident.urn,
                input: { stage: formData?.status, message: formData?.note, state: IncidentState.Resolved },
            },
        })
            .then(() => {
                message.destroy();
                analytics.event({
                    type: EventType.EntityActionEvent,
                    entityType,
                    entityUrn: incident.urn,
                    actionType: EntityActionType.ResolvedIncident,
                });

                const values = {
                    title: incident.title,
                    description: incident.description,
                    type: incident.type,
                    priority: incident.priority,
                    state: IncidentState.Resolved,
                    customType: incident.customType,
                    stage: formData?.status || IncidentStage.Fixed,
                    message: formData?.note,
                    linkedAssets: incident.linkedAssets,
                    assignees: incident.assignees,
                };

                const updatedIncident = getCacheIncident({
                    values,
                    incidentUrn: incident.urn,
                    user,
                });

                updateActiveIncidentInCache(client, urn, updatedIncident, PAGE_SIZE);
                message.success({ content: 'Incident updated!', duration: 2 });
                handleClose?.();
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: 'Failed to update incident! An unexpected error occurred',
                    permissionMessage:
                        'Unauthorized to update incident for this asset. Please contact your DataHub administrator.',
                });
            });
    };

    return (
        <Modal
            title={<ModalTitle />}
            visible
            destroyOnClose
            onCancel={handleClose}
            centered
            width={500}
            style={modalBodyStyle}
            footer={
                <ModalButtonContainer>
                    <Button key="cancel" variant="text" onClick={handleClose}>
                        Cancel
                    </Button>
                    <Button form="resolveIncident" key="submit" type="submit" data-testid="incident-save-button">
                        Save
                    </Button>
                </ModalButtonContainer>
            }
        >
            <Form
                form={form}
                name="resolveIncident"
                onFinish={handleResolveIncident}
                layout="vertical"
                initialValues={{ status: IncidentStage.Fixed }}
            >
                <IncidentSelectField
                    incidentLabelMap={INCIDENT_OPTION_LABEL_MAPPING.stage}
                    options={INCIDENT_RESOLUTION_STAGES}
                    onUpdate={(value) => form.setFieldsValue({ status: value })}
                    form={form}
                    handleValuesChange={handleValuesChange}
                    showClear={false}
                    width="100%"
                    customStyle={{ flexDirection: 'column', alignItems: 'normal' }}
                    value={form.getFieldValue(INCIDENT_OPTION_LABEL_MAPPING.stage.fieldName)}
                />
                <FormItem
                    name="note"
                    label="Note"
                    rules={[
                        {
                            required: false,
                            message: 'A note is required.',
                        },
                    ]}
                    style={{ color: colors.gray[600] }}
                >
                    <TextArea
                        rows={4}
                        placeholder="Add a resolved note - optional"
                        data-testid="incident-resolve-note-input"
                    />
                </FormItem>
            </Form>
        </Modal>
    );
};
