import React, { useState } from 'react';
import { Button as AntButton, message, Modal, Form, Input, Checkbox } from 'antd';
import styled from 'styled-components/macro';
import { PlusOutlined } from '@ant-design/icons';
import { Button } from '@src/alchemy-components';
import { useEntityData, useMutationUrn } from '../../../../entity/shared/EntityContext';
import { useAddLinkMutation } from '../../../../../graphql/mutations.generated';
import analytics, { EventType, EntityActionType } from '../../../../analytics';
import { useUserContext } from '../../../../context/useUserContext';

const FooterContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
`;

const FooterActions = styled.div``;

type AddLinkProps = {
    buttonProps?: Record<string, unknown>;
    refetch?: () => Promise<any>;
    buttonType?: string;
};

export const AddLinkModal = ({ buttonProps, refetch, buttonType }: AddLinkProps) => {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [isShowInAssetPreview, setIsShowInAssetPreview] = useState<boolean>(false);
    const mutationUrn = useMutationUrn();
    const user = useUserContext();
    const { entityType } = useEntityData();
    const [addLinkMutation] = useAddLinkMutation();

    const [form] = Form.useForm();

    const showModal = () => {
        setIsModalVisible(true);
        setIsShowInAssetPreview(false);
    };

    const handleClose = () => {
        form.resetFields();
        setIsModalVisible(false);
        setIsShowInAssetPreview(false);
    };

    const handleAdd = async (formData: any) => {
        if (user?.urn) {
            try {
                await addLinkMutation({
                    variables: {
                        input: {
                            linkUrl: formData.url,
                            label: formData.label,
                            settings: { showInAssetPreview: isShowInAssetPreview },
                            resourceUrn: mutationUrn,
                        },
                    },
                });
                message.success({ content: 'Link Added', duration: 2 });
                analytics.event({
                    type: EventType.EntityActionEvent,
                    entityType,
                    entityUrn: mutationUrn,
                    actionType: EntityActionType.UpdateLinks,
                });
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to add link: \n ${e.message || ''}`, duration: 3 });
                }
            }
            refetch?.();
            handleClose();
        } else {
            message.error({ content: `Error adding link: no user`, duration: 2 });
        }
    };

    const renderButton = (bType: string | undefined) => {
        if (bType === 'transparent') {
            return (
                <Button data-testid="add-link-button" variant="outline" onClick={showModal} {...buttonProps}>
                    <PlusOutlined />
                    Add Link
                </Button>
            );
        }
        if (bType === 'text') {
            return (
                <AntButton data-testid="add-link-button" onClick={showModal} type="text">
                    <PlusOutlined />
                    Add Link
                </AntButton>
            );
        }
        return (
            <Button variant="outline" data-testid="add-link-button" onClick={showModal} {...buttonProps}>
                <PlusOutlined />
                Add Link
            </Button>
        );
    };

    return (
        <>
            {renderButton(buttonType)}
            <Modal
                title="Add Link"
                visible={isModalVisible}
                destroyOnClose
                onCancel={handleClose}
                footer={
                    <FooterContainer>
                        <Checkbox
                            checked={isShowInAssetPreview}
                            onChange={(e) => setIsShowInAssetPreview(e.target.checked)}
                        >
                            Add to asset header
                        </Checkbox>
                        <FooterActions>
                            <Button variant="text" onClick={handleClose}>
                                Cancel
                            </Button>
                            <Button
                                data-testid="add-link-modal-add-button"
                                form="addLinkForm"
                                key="submit"
                                type="submit"
                            >
                                Add
                            </Button>
                        </FooterActions>
                    </FooterContainer>
                }
            >
                <Form form={form} name="addLinkForm" onFinish={handleAdd} layout="vertical">
                    <Form.Item
                        data-testid="add-link-modal-url"
                        name="url"
                        label="URL"
                        rules={[
                            {
                                required: true,
                                message: 'A URL is required.',
                            },
                            {
                                type: 'url',
                                warningOnly: true,
                                message: 'This field must be a valid url.',
                            },
                        ]}
                    >
                        <Input placeholder="https://" autoFocus />
                    </Form.Item>
                    <Form.Item
                        data-testid="add-link-modal-label"
                        name="label"
                        label="Label"
                        rules={[
                            {
                                required: true,
                                message: 'A label is required.',
                            },
                        ]}
                    >
                        <Input placeholder="A short label for this link" />
                    </Form.Item>
                </Form>
            </Modal>
        </>
    );
};
