import React, { useState } from 'react';
import { message, Modal, Button, Form, Select } from 'antd';
import { MinusOutlined } from '@ant-design/icons';
import { useEntityData, useMutationUrn } from '../../EntityContext';
import { useRemoveLinkMutation } from '../../../../../graphql/mutations.generated';
import analytics, { EventType, EntityActionType } from '../../../../analytics';
import { useUserContext } from '../../../../context/useUserContext';
import { InstitutionalMemoryMetadata } from '../../../../../types.generated';

type RemoveLinksProps = {
    buttonProps?: Record<string, unknown>;
    refetch?: () => Promise<any>;
};

export const RemoveLinkModal = ({ buttonProps, refetch }: RemoveLinksProps) => {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [selectedLink, setSelectedLink] = useState('');
    const mutationUrn = useMutationUrn();
    const user = useUserContext();
    const { entityData, entityType } = useEntityData();
    const [removeLinkMutation] = useRemoveLinkMutation();
    const links = entityData?.institutionalMemory?.elements || [];

    const [form] = Form.useForm();

    const showModal = () => {
        if (links.length !== 0) {
            setIsModalVisible(true);
        } else {
            message.error({ content: `No links to remove`, duration: 2 });
        }
    };

    const handleClose = () => {
        form.resetFields();
        setIsModalVisible(false);
    };

    const linkSearchOptions = links?.map((result: InstitutionalMemoryMetadata) => {
        return { value: result.url, label: result?.description || result?.label };
    });

    const onSelectLink = (linkUrl: string) => {
        setSelectedLink(linkUrl);
    };
    const onDeselectLink = () => {
        setSelectedLink('');
    };
    const handleRemove = async () => {
        if (user?.urn) {
            try {
                if (selectedLink === '') {
                    message.error({ content: `Error removing link: no link selected`, duration: 2 });
                    return;
                }
                await removeLinkMutation({
                    variables: { input: { linkUrl: selectedLink, resourceUrn: mutationUrn } },
                });
                message.success({ content: 'Link removed!', duration: 2 });
                analytics.event({
                    type: EventType.EntityActionEvent,
                    entityType,
                    entityUrn: mutationUrn,
                    actionType: EntityActionType.UpdateLinks,
                });
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to remove link: \n ${e.message || ''}`, duration: 3 });
                }
            }
            refetch?.();
            handleClose();
        } else {
            message.error({ content: `Error removing link: no user`, duration: 2 });
        }
    };

    return (
        <>
            <Button icon={<MinusOutlined />} onClick={showModal} {...buttonProps}>
                Remove Link
            </Button>
            <Modal
                title="Remove Link"
                visible={isModalVisible}
                destroyOnClose
                onCancel={handleClose}
                footer={[
                    <Button type="text" onClick={handleClose}>
                        Cancel
                    </Button>,
                    <Button form="removeLinkForm" key="submit" htmlType="submit" disabled={selectedLink === ''}>
                        Remove
                    </Button>,
                ]}
            >
                <Form form={form} name="removeLinkForm" onFinish={handleRemove} layout="vertical">
                    <Form.Item>
                        <Select
                            filterOption={false}
                            showSearch={false}
                            defaultActiveFirstOption={false}
                            placeholder="Select a link to remove"
                            onSelect={(linkUrl: any) => onSelectLink(linkUrl)}
                            onDeselect={onDeselectLink}
                            options={linkSearchOptions}
                        />
                    </Form.Item>
                </Form>
            </Modal>
        </>
    );
};
