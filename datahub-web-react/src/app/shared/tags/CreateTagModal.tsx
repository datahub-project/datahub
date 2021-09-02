import React, { useState } from 'react';
import { FetchResult } from '@apollo/client';
import { Button, Input, Modal, Space } from 'antd';
import styled from 'styled-components';

import { UpdateDatasetMutation } from '../../../graphql/dataset.generated';
import { useUpdateTagMutation } from '../../../graphql/tag.generated';
import { GlobalTags, GlobalTagsUpdate, TagAssociationUpdate } from '../../../types.generated';
import { convertTagsForUpdate } from './utils/convertTagsForUpdate';

type CreateTagModalProps = {
    globalTags?: GlobalTags | null;
    updateTags?: (
        update: GlobalTagsUpdate,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>>>;
    visible: boolean;
    onClose: () => void;
    onBack: () => void;
    tagName: string;
};

const FullWidthSpace = styled(Space)`
    width: 100%;
`;

export default function CreateTagModal({
    updateTags,
    globalTags,
    onClose,
    onBack,
    visible,
    tagName,
}: CreateTagModalProps) {
    const [stagedDescription, setStagedDescription] = useState('');

    const [updateTagMutation] = useUpdateTagMutation();
    const [disableCreate, setDisableCreate] = useState(false);

    const onOk = () => {
        setDisableCreate(true);
        // first create the new tag
        updateTagMutation({
            variables: {
                input: {
                    urn: `urn:li:tag:${tagName}`,
                    name: tagName,
                    description: stagedDescription,
                },
            },
        }).then(() => {
            // then apply the tag to the dataset
            updateTags?.({
                tags: [
                    ...convertTagsForUpdate(globalTags?.tags || []),
                    { tag: { urn: `urn:li:tag:${tagName}`, name: tagName } },
                ] as TagAssociationUpdate[],
            }).finally(() => {
                // and finally close the modal
                setDisableCreate(false);
                onClose();
            });
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
