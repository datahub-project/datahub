import { PlusOutlined } from '@ant-design/icons';
import { Button as AntButton, message } from 'antd';
import React, { useState } from 'react';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { useEntityData, useMutationUrn } from '@app/entity/shared/EntityContext';
import { FormData, LinkFormModal } from '@app/entityV2/shared/components/styled/LinkFormModal';
import { Button } from '@src/alchemy-components';

import { useAddLinkMutation } from '@graphql/mutations.generated';

interface Props {
    buttonProps?: Record<string, unknown>;
    refetch?: () => Promise<any>;
    buttonType?: string;
}

export const AddLinkModal = ({ buttonProps, refetch, buttonType }: Props) => {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const mutationUrn = useMutationUrn();
    const user = useUserContext();
    const { entityType } = useEntityData();
    const [addLinkMutation] = useAddLinkMutation();

    const showModal = () => {
        setIsModalVisible(true);
    };

    const handleClose = () => {
        setIsModalVisible(false);
    };

    const handleAdd = async (formData: FormData) => {
        if (user?.urn) {
            try {
                await addLinkMutation({
                    variables: {
                        input: {
                            linkUrl: formData.url,
                            label: formData.label,
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
                handleClose();
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to add link: \n ${e.message || ''}`, duration: 3 });
                }
            }
            refetch?.();
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
            <LinkFormModal variant="create" open={isModalVisible} onSubmit={handleAdd} onCancel={handleClose} />
        </>
    );
};
