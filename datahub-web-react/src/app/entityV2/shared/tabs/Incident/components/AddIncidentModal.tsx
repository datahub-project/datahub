import React, { useState } from 'react';
import { message, Modal, Button, Form, Input, Typography, Select } from 'antd';
import { useApolloClient } from '@apollo/client';
import styled from 'styled-components';
import { Editor } from '@src/app/entity/shared/tabs/Documentation/components/editor/Editor';
import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { EntityType, IncidentSourceType, IncidentState, IncidentType } from '../../../../../../types.generated';
import { INCIDENT_DISPLAY_TYPES, PAGE_SIZE, addActiveIncidentToCache } from '../incidentUtils';
import { useRaiseIncidentMutation } from '../../../../../../graphql/mutations.generated';
import handleGraphQLError from '../../../../../shared/handleGraphQLError';
import { useUserContext } from '../../../../../context/useUserContext';

const StyledEditor = styled(Editor)`
    border: 1px solid ${ANTD_GRAY[4.5]};
`;

type AddIncidentProps = {
    urn: string;
    entityType: EntityType;
    visible: boolean;
    onClose?: () => void;
    refetch?: () => void;
};

export const AddIncidentModal = ({ urn, entityType, visible, onClose, refetch }: AddIncidentProps) => {
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
                message.success({ content: 'Incident Added', duration: 2 });
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
                title="Raise Incident"
                visible={visible}
                destroyOnClose
                onCancel={handleClose}
                width={600}
                footer={[
                    <Button type="text" onClick={handleClose}>
                        Cancel
                    </Button>,
                    <Button type="primary" form="addIncidentForm" key="submit" htmlType="submit">
                        Raise
                    </Button>,
                ]}
            >
                <Form form={form} name="addIncidentForm" onFinish={handleAddIncident} layout="vertical">
                    <Form.Item label={<Typography.Text strong>Type</Typography.Text>}>
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
                            label="Custom Type"
                            rules={[
                                {
                                    required: selectedIncidentType === IncidentType.Custom,
                                    message: 'A custom type is required.',
                                },
                            ]}
                        >
                            <Input placeholder="Freshness" />
                        </Form.Item>
                    )}
                    <Form.Item
                        name="title"
                        label="Title"
                        rules={[
                            {
                                required: true,
                                message: 'A title is required.',
                            },
                        ]}
                    >
                        <Input placeholder="What went wrong?" />
                    </Form.Item>
                    <Form.Item
                        name="description"
                        label="Description"
                        rules={[
                            {
                                required: true,
                                message: 'A description is required.',
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
        </>
    );
};
