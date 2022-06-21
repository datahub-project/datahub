import React, { useState } from 'react';
import { message, Modal, Button, Form, Input, Typography, Select } from 'antd';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { useEntityData } from '../../../EntityContext';
import { IncidentType } from '../../../../../../types.generated';
import { INCIDENT_DISPLAY_TYPES } from '../incidentUtils';
import { useRaiseIncidentMutation } from '../../../../../../graphql/mutations.generated';

type AddIncidentProps = {
    visible: boolean;
    onClose?: () => void;
    refetch?: () => Promise<any>;
};

export const AddIncidentModal = ({ visible, onClose, refetch }: AddIncidentProps) => {
    const { urn, entityType } = useEntityData();
    const incidentTypes = INCIDENT_DISPLAY_TYPES;
    const [selectedIncidentType, setSelectedIncidentType] = useState<IncidentType>(IncidentType.Operational);
    const [isOtherTypeSelected, setIsOtherTypeSelected] = useState<boolean>(false);
    const [raiseIncidentMutation] = useRaiseIncidentMutation();

    const [form] = Form.useForm();

    const handleClose = () => {
        form.resetFields();
        setIsOtherTypeSelected(false);
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
        try {
            await raiseIncidentMutation({
                variables: {
                    input: {
                        type: selectedIncidentType,
                        title: formData.title,
                        description: formData.description,
                        resourceUrn: urn,
                        customType: formData.customType,
                    },
                },
            });
            message.success({ content: 'Incident Added', duration: 2 });
            analytics.event({
                type: EventType.EntityActionEvent,
                entityType,
                entityUrn: urn,
                actionType: EntityActionType.AddIncident,
            });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to add incident: \n ${e.message || ''}`, duration: 3 });
            }
        }
        handleClose();
        setTimeout(function () {
            refetch?.();
        }, 2000);
    };

    return (
        <>
            <Modal
                title="Raise Incident"
                visible={visible}
                destroyOnClose
                onCancel={handleClose}
                footer={[
                    <Button type="text" onClick={handleClose}>
                        Cancel
                    </Button>,
                    <Button form="addIncidentForm" key="submit" htmlType="submit">
                        Add
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
                            <Input />
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
                        <Input placeholder="A title for this incident" />
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
                        <Input placeholder="A short description for this incident" />
                    </Form.Item>
                </Form>
            </Modal>
        </>
    );
};
