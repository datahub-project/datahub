import { useApolloClient } from '@apollo/client';
import { Button, Form, Input, Modal, Select, Typography, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import {
    INCIDENT_DISPLAY_TYPES,
    PAGE_SIZE,
    updateActiveIncidentInCache,
} from '@app/entityV2/shared/tabs/Incident/incidentUtils';
import handleGraphQLError from '@app/shared/handleGraphQLError';
import { Editor } from '@src/app/entity/shared/tabs/Documentation/components/editor/Editor';

import { useRaiseIncidentMutation } from '@graphql/mutations.generated';
import { EntityType, IncidentSourceType, IncidentState, IncidentType } from '@types';

const StyledEditor = styled(Editor)`
    border: 1px solid ${(props) => props.theme.colors.border};
`;

type AddIncidentProps = {
    urn: string;
    entityType: EntityType;
    visible: boolean;
    onClose?: () => void;
    refetch?: () => void;
};

export const AddIncidentModal = ({ urn, entityType, visible, onClose, refetch }: AddIncidentProps) => {
    const { t } = useTranslation('entity.profile.incident');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');
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
                    startedAt: null,
                    tags: null,
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
                message.success({ content: t('toast.incidentAdded'), duration: 2 });
                analytics.event({
                    type: EventType.EntityActionEvent,
                    entityType,
                    entityUrn: urn,
                    actionType: EntityActionType.AddIncident,
                });
                updateActiveIncidentInCache(client, urn, newIncident, PAGE_SIZE);
                handleClose();
                setTimeout(() => {
                    refetch?.();
                }, 2000);
            })
            .catch((error) => {
                console.error(error);
                handleGraphQLError({
                    error,
                    defaultMessage: t('toast.raiseFailedUnexpected'),
                    permissionMessage: t('toast.raiseUnauthorizedAsset'),
                });
            });
    };

    return (
        <Modal
            title={t('modal.title')}
            visible={visible}
            destroyOnClose
            onCancel={handleClose}
            width={600}
            footer={[
                <Button type="text" onClick={handleClose}>
                    {tc('cancel')}
                </Button>,
                <Button type="primary" form="addIncidentForm" key="submit" htmlType="submit">
                    {t('modal.raiseButton')}
                </Button>,
            ]}
        >
            <Form form={form} name="addIncidentForm" onFinish={handleAddIncident} layout="vertical">
                <Form.Item label={<Typography.Text strong>{tl('type')}</Typography.Text>}>
                    <Form.Item name="type" style={{ marginBottom: '0px' }}>
                        <Select
                            value={selectedIncidentType}
                            onChange={onSelectIncidentType}
                            defaultValue={IncidentType.Operational}
                            autoFocus
                        >
                            {incidentTypes.map((incidentType) => (
                                <Select.Option key={incidentType.type} value={incidentType.type}>
                                    <Typography.Text>{incidentType.name}</Typography.Text>
                                </Select.Option>
                            ))}
                        </Select>
                    </Form.Item>
                </Form.Item>
                {isOtherTypeSelected && (
                    <Form.Item
                        name="customType"
                        label={t('modal.customTypeLabel')}
                        rules={[
                            {
                                required: selectedIncidentType === IncidentType.Custom,
                                message: t('modal.customTypeRequired'),
                            },
                        ]}
                    >
                        <Input placeholder={t('modal.typePlaceholder')} />
                    </Form.Item>
                )}
                <Form.Item
                    name="title"
                    label={tl('title')}
                    rules={[
                        {
                            required: true,
                            message: t('modal.titleRequired'),
                        },
                    ]}
                >
                    <Input placeholder={t('modal.descriptionPlaceholder')} />
                </Form.Item>
                <Form.Item
                    name="description"
                    label={tl('description')}
                    rules={[
                        {
                            required: true,
                            message: t('modal.descriptionRequired'),
                        },
                    ]}
                >
                    <StyledEditor
                        doNotFocus
                        className="add-incident-description"
                        onKeyDown={(e) => {
                            // Preventing the modal from closing when the Enter key is pressed
                            if (e.key === 'Enter') {
                                e.preventDefault();
                                e.stopPropagation();
                            }
                        }}
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
};
