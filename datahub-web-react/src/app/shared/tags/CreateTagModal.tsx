import { Button, Input, Modal, Space, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { handleBatchError } from '@app/entity/shared/utils';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';

import { useBatchAddTagsMutation } from '@graphql/mutations.generated';
import { useCreateTagMutation } from '@graphql/tag.generated';
import { ResourceRefInput } from '@types';

type CreateTagModalProps = {
    open: boolean;
    onClose: () => void;
    onBack: () => void;
    tagName: string;
    resources: ResourceRefInput[];
};

const FullWidthSpace = styled(Space)`
    width: 100%;
`;

export default function CreateTagModal({ onClose, onBack, open, tagName, resources }: CreateTagModalProps) {
    const { t } = useTranslation('shared.tags');
    const { t: tc } = useTranslation('common.actions');
    const [stagedDescription, setStagedDescription] = useState('');
    const [batchAddTagsMutation] = useBatchAddTagsMutation();

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
                batchAddTagsMutation({
                    variables: {
                        input: {
                            tagUrns: [tagUrn],
                            resources,
                        },
                    },
                })
                    .catch((e) => {
                        message.destroy();
                        message.error(
                            handleBatchError(resources, e, {
                                content: t('createTag.addError', { error: e.message || '' }),
                                duration: 3,
                            }),
                        );
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
                message.error({ content: t('createTag.error', { error: e.message || '' }), duration: 3 });
                onClose();
            });
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createTagButton',
    });

    return (
        <Modal
            title={t('createTag.title', { tagName })}
            open={open}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onBack} type="text">
                        {tc('back')}
                    </Button>
                    <Button id="createTagButton" onClick={onOk} disabled={disableCreate}>
                        {tc('create')}
                    </Button>
                </>
            }
        >
            <FullWidthSpace direction="vertical">
                <Input.TextArea
                    placeholder={t('createTag.descriptionPlaceholder')}
                    value={stagedDescription}
                    onChange={(e) => setStagedDescription(e.target.value)}
                />
            </FullWidthSpace>
        </Modal>
    );
}
