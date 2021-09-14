import React, { useState } from 'react';
import { message, Button, Input, Modal, Space } from 'antd';
import styled from 'styled-components';

import { useUpdateTagMutation } from '../../../graphql/tag.generated';
import { useAddTagMutation } from '../../../graphql/mutations.generated';

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

    const [updateTagMutation] = useUpdateTagMutation();
    const [disableCreate, setDisableCreate] = useState(false);

    const onOk = () => {
        setDisableCreate(true);
        // first create the new tag
        const tagUrn = `urn:li:tag:${tagName}`;
        updateTagMutation({
            variables: {
                input: {
                    urn: tagUrn,
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
                            targetUrn: entityUrn,
                            subResource: entitySubresource,
                        },
                    },
                }).finally(() => {
                    // and finally close the modal
                    setDisableCreate(false);
                    onClose();
                });
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to remove term: \n ${e.message || ''}`, duration: 3 });
                onClose();
            });
    };

    return (
        <Modal
            title={`Create ${tagName}`}
            visible={visible}
            footer={
                <>
                    <Button onClick={onBack} type="text">
                        Back
                    </Button>
                    <Button onClick={onOk} disabled={stagedDescription.length === 0 || disableCreate}>
                        Create
                    </Button>
                </>
            }
        >
            <FullWidthSpace direction="vertical">
                <Input.TextArea
                    placeholder="Write a description for your new tag..."
                    value={stagedDescription}
                    onChange={(e) => setStagedDescription(e.target.value)}
                />
            </FullWidthSpace>
        </Modal>
    );
}
