import React, { useState } from 'react';
import { message, Modal, Button, Form, Input, Typography, Select } from 'antd';
import { useApolloClient } from '@apollo/client';
import TextArea from 'antd/lib/input/TextArea';
import { useTranslation } from 'react-i18next';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { useEntityData } from '../../../EntityContext';
import { EntityType, IncidentSourceType, IncidentState, IncidentType } from '../../../../../../types.generated';
import { INCIDENT_DISPLAY_TYPES, PAGE_SIZE, addActiveIncidentToCache } from '../incidentUtils';
import { useRaiseIncidentMutation } from '../../../../../../graphql/mutations.generated';
import handleGraphQLError from '../../../../../shared/handleGraphQLError';
import { useUserContext } from '../../../../../context/useUserContext';
import { translateDisplayNames } from '../../../../../../utils/translation/translation';

type AddIncidentProps = {
    visible: boolean;
    onClose?: () => void;
    refetch?: () => Promise<any>;
};

export const AddIncidentModal = ({ visible, onClose, refetch }: AddIncidentProps) => {
    const { t } = useTranslation();
    const { urn, entityType } = useEntityData();
    const { user } = useUserContext();
    const incidentTypes = INCIDENT_DISPLAY_TYPES;
    const [selectedIncidentType, setSelectedIncidentType] = useState<IncidentType>(IncidentType.Operational);
    const [isOtherTypeSelected, setIsOtherTypeSelected] = useState<boolean>(false);
    const [raiseIncidentMutation] = useRaiseIncidentMutation();

    const client = useApolloClient();
    const [form] = Form.useForm();

    const handleClose = () => {
        form.resetFields();
        setIsOtherTypeSelected(false);
        setSelectedIncidentType(IncidentType.Operational);
        onClose?.();
    };

    const onSelectIncidentType = (newType) => {
        if (newType === 'OTHER') {
            setIsOtherTypeSelected(true);
            setSelectedIncidentType(IncidentType.Custom);
        } else {
            setIsOtherTypeSelected(false);
            setSelectedIncidentType(newType);
        }
    };

    const handleAddIncident = async (formData: any) => {
        raiseIncidentMutation({
            variables: {
                input: {
                    type: selectedIncidentType,
                    title: formData.title,
                    description: formData.description,
                    resourceUrn: urn,
                    customType: formData.customType,
                },
            },
        })
            .then(({ data }) => {
                const newIncident = {
                    urn: data?.raiseIncident,
                    type: EntityType.Incident,
                    incidentType: selectedIncidentType,
                    customType: formData.customType || null,
                    title: formData.title,
                    description: formData.description,
                    status: {
                        state: IncidentState.Active,
                        message: null,
                        lastUpdated: {
                            __typename: 'AuditStamp',
                            time: Date.now(),
                            actor: user?.urn,
                        },
                    },
                    source: {
                        type: IncidentSourceType.Manual,
                    },
                    created: {
                        time: Date.now(),
                        actor: user?.urn,
                    },
                };
                message.success({ content: t('common.incidentAdded'), duration: 2 });
                analytics.event({
                    type: EventType.EntityActionEvent,
                    entityType,
                    entityUrn: urn,
                    actionType: EntityActionType.AddIncident,
                });
                addActiveIncidentToCache(client, urn, newIncident, PAGE_SIZE);
                handleClose();
                setTimeout(() => {
                    refetch?.();
                }, 2000);
            })
            .catch((error) => {
                console.error(error);
                handleGraphQLError({
                    error,
                    defaultMessage: 'Failed to raise incident! An unexpected error occurred',
                    permissionMessage:
                        'Unauthorized to raise incident for this asset. Please contact your DataHub administrator.',
                });
            });
    };

    return (
        <>
            <Modal
                title={t('deprecation.raiseIncident')}
                visible={visible}
                destroyOnClose
                onCancel={handleClose}
                footer={[
                    <Button type="text" onClick={handleClose}>
                        {t('common.cancel')}
                    </Button>,
                    <Button form="addIncidentForm" key="submit" htmlType="submit">
                        {t('common.add')}
                    </Button>,
                ]}
            >
                <Form form={form} name="addIncidentForm" onFinish={handleAddIncident} layout="vertical">
                    <Form.Item label={<Typography.Text strong>{t('common.type')}</Typography.Text>}>
                        <Form.Item name="type" style={{ marginBottom: '0px' }}>
                            <Select
                                value={selectedIncidentType}
                                onChange={onSelectIncidentType}
                                defaultValue={IncidentType.Operational}
                                autoFocus
                            >
                                {incidentTypes.map((incidentType) => (
                                    <Select.Option key={incidentType.type} value={incidentType.type}>
                                        <Typography.Text>
                                            {translateDisplayNames(t, `incident${incidentType.name}`)}
                                        </Typography.Text>
                                    </Select.Option>
                                ))}
                            </Select>
                        </Form.Item>
                    </Form.Item>
                    {isOtherTypeSelected && (
                        <Form.Item
                            name="customType"
                            label={t('incident.customType')}
                            rules={[
                                {
                                    required: selectedIncidentType === IncidentType.Custom,
                                    message: t('form.requiredWithName', { field: t('incident.customType') }),
                                },
                            ]}
                        >
                            <Input placeholder={t('incident.customTypePlaceholder')} />
                        </Form.Item>
                    )}
                    <Form.Item
                        name="title"
                        label={t('common.title')}
                        rules={[
                            {
                                required: true,
                                message: t('form.requiredWithName', { field: t('common.title') }),
                            },
                        ]}
                    >
                        <Input placeholder={t('error.errorSection.somethingWentWrong')} />
                    </Form.Item>
                    <Form.Item
                        name="description"
                        label={t('common.description')}
                        rules={[
                            {
                                required: true,
                                message: t('form.requiredWithName', { field: t('common.description') }),
                            },
                        ]}
                    >
                        <TextArea placeholder={t('form.additionalDetails')} />
                    </Form.Item>
                </Form>
            </Modal>
        </>
    );
};
