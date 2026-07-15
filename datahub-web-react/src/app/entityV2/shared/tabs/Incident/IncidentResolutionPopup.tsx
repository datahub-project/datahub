import { Form, Input, Modal, message } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';

import { IncidentSelectField } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentSelectedField';
import { INCIDENT_OPTION_LABEL_MAPPING, INCIDENT_RESOLUTION_STAGES } from '@app/entityV2/shared/tabs/Incident/constant';
import { FormItem, ModalHeading, ModalTitleContainer } from '@app/entityV2/shared/tabs/Incident/styledComponents';
import { IncidentTableRow } from '@app/entityV2/shared/tabs/Incident/types';
import { Button } from '@src/alchemy-components';
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

const ModalTitle = () => {
    const { t } = useTranslation('entity.profile.incident');
    return (
        <ModalTitleContainer>
            <ModalHeading>{t('resolution.title')}</ModalHeading>
        </ModalTitleContainer>
    );
};

export const IncidentResolutionPopup = ({ incident, refetch, handleClose }: IncidentResolutionPopupProps) => {
    const { t } = useTranslation('entity.profile.incident');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const theme = useTheme();
    const { urn, entityType } = useEntityData();
    const [updateIncidentStatusMutation] = useUpdateIncidentStatusMutation();
    const [form] = Form.useForm();
    const formValues = Form.useWatch([], form);

    const handleValuesChange = (changedValues: any) => {
        Object.keys(changedValues).forEach((fieldName) => form.setFields([{ name: fieldName, errors: [] }]));
    };

    const handleResolveIncident = (formData: any) => {
        message.loading({ content: tf('updating') });

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
                message.success({ content: t('resolution.success'), duration: 2 });
                refetch();
                handleClose?.();
            })
            .catch((error) => {
                console.log(error);
                handleGraphQLError({
                    error,
                    defaultMessage: t('resolution.updateFailed'),
                    permissionMessage: t('resolution.updateUnauthorizedAsset'),
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
                        {tc('cancel')}
                    </Button>
                    <Button form="resolveIncident" key="submit" type="submit" data-testid="incident-save-button">
                        {tc('save')}
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
                    label={t('resolution.noteLabel')}
                    rules={[
                        {
                            required: false,
                            message: t('resolution.noteRequired'),
                        },
                    ]}
                    style={{ color: theme.colors.text }}
                >
                    <TextArea
                        rows={4}
                        placeholder={t('resolution.notePlaceholder')}
                        data-testid="incident-resolve-note-input"
                    />
                </FormItem>
            </Form>
        </Modal>
    );
};
