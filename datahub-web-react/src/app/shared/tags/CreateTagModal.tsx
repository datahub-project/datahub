import React, { useState } from 'react';
import { message, Button, Input, Modal, Space } from 'antd';
import styled from 'styled-components';
import { useAddTagMutation } from '../../../graphql/mutations.generated';
import { useCreateTagMutation } from '../../../graphql/tag.generated';
import { SubResourceType } from '../../../types.generated';
import { useEnterKeyListener } from '../useEnterKeyListener';

type CreateTagModalProps = {
    visible: boolean;
    onClose: () => void;
    onBack: () => void;
    tagName: string;
    entityUrn: string;
    entitySubresource?: string;
};

const FullWidthSpace = styled(Space)`
    width: 100%;
`;

export default function CreateTagModal({
    onClose,
    onBack,
    visible,
    tagName,
    entityUrn,
    entitySubresource,
}: CreateTagModalProps) {
    const [stagedDescription, setStagedDescription] = useState('');
    const [addTagMutation] = useAddTagMutation();

    const [createTagMutation] = useCreateTagMutation();
    const [disableCreate, setDisableCreate] = useState(false);

    const onOk = () => {
        setDisableCreate(true);
        // first create the new tag
        const tagUrn = `urn:li:tag:${tagName}`;
        createTagMutation({
            variables: {
                input: {
                    id: tagName,
                    name: tagName,
                    description: stagedDescription,
                },
            },
        })
            .then(() => {
                // then apply the tag to the dataset
                addTagMutation({
                    variables: {
                        input: {
                            tagUrn,
                            resourceUrn: entityUrn,
                            subResource: entitySubresource,
                            subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                        },
                    },
                })
                    .catch((e) => {
                        message.destroy();
                        message.error({ content: `Failed to add tag: \n ${e.message || ''}`, duration: 3 });
                        onClose();
                    })
                    .finally(() => {
                        // and finally close the modal
                        setDisableCreate(false);
                        onClose();
                    });
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create tag: \n ${e.message || ''}`, duration: 3 });
                onClose();
            });
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createTagButton',
    });

    return (
        <Modal
            title={`Create ${tagName}`}
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onBack} type="text">
                        Back
                    </Button>
                    <Button id="createTagButton" onClick={onOk} disabled={disableCreate}>
                        Create
                    </Button>
                </>
            }
        >
            <FullWidthSpace direction="vertical">
                <Input.TextArea
                    placeholder="Add a description for your new tag..."
                    value={stagedDescription}
                    onChange={(e) => setStagedDescription(e.target.value)}
                />
            </FullWidthSpace>
        </Modal>
    );
}
