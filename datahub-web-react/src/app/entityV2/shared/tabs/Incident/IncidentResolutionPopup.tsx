import { Form, Input, Modal, message } from 'antd';
import React from 'react';

import { IncidentSelectField } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentSelectedField';
import { INCIDENT_OPTION_LABEL_MAPPING, INCIDENT_RESOLUTION_STAGES } from '@app/entityV2/shared/tabs/Incident/constant';
import { FormItem, ModalHeading, ModalTitleContainer } from '@app/entityV2/shared/tabs/Incident/styledComponents';
import { IncidentTableRow } from '@app/entityV2/shared/tabs/Incident/types';
import { Button, colors } from '@src/alchemy-components';
import analytics, { EntityActionType, EventType } from '@src/app/analytics';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import handleGraphQLError from '@src/app/shared/handleGraphQLError';
import { useUpdateIncidentStatusMutation } from '@src/graphql/mutations.generated';
import { IncidentStage, IncidentState } from '@src/types.generated';

type IncidentResolutionPopupProps = {
    incident: IncidentTableRow;
    refetch: () => void;
    handleClose: () => void;
};

const { TextArea } = Input;

const modalBodyStyle = { fontFamily: 'Mulish, sans-serif' };

const ModalTitle = () => (
    <ModalTitleContainer>
        <ModalHeading>Resolve Incident</ModalHeading>
    </ModalTitleContainer>
);

export const IncidentResolutionPopup = ({ incident, refetch, handleClose }: IncidentResolutionPopupProps) => {
    const { urn, entityType } = useEntityData();
    const [updateIncidentStatusMutation] = useUpdateIncidentStatusMutation();
    const [form] = Form.useForm();
    const formValues = Form.useWatch([], form);

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
                    entityUrn: urn,
                    actionType: EntityActionType.ResolvedIncident,
                });
                message.success({ content: 'Incident updated!', duration: 2 });
                refetch();
                handleClose?.();
            })
            .catch((error) => {
                console.log(error);
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
                    value={formValues?.[INCIDENT_OPTION_LABEL_MAPPING.stage.fieldName]}
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
