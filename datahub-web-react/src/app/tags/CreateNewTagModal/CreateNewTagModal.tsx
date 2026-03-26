import { Modal } from '@components';
import { message } from 'antd';
import React, { useState } from 'react';
import { useTheme } from 'styled-components';

import { ModalButton } from '@components/components/Modal/Modal';

import OwnersSection from '@app/domainV2/OwnersSection';
import { createOwnerInputs } from '@app/entityV2/shared/utils/selectorUtils';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import TagDetailsSection from '@app/tags/CreateNewTagModal/TagDetailsSection';

import { useBatchAddOwnersMutation, useSetTagColorMutation } from '@graphql/mutations.generated';
import { useCreateTagMutation } from '@graphql/tag.generated';

type CreateNewTagModalProps = {
    open: boolean;
    onClose: () => void;
};

const CreateNewTagModal: React.FC<CreateNewTagModalProps> = ({ onClose, open }) => {
    const theme = useTheme();
    const defaultTagColor = theme.colors.textBrand;
    const [tagName, setTagName] = useState('');
    const [tagDescription, setTagDescription] = useState('');
    const [tagColor, setTagColor] = useState(defaultTagColor);
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [isLoading, setIsLoading] = useState(false);

    const [createTagMutation] = useCreateTagMutation();
    const [setTagColorMutation] = useSetTagColorMutation();
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();

    const onOk = async () => {
        if (!tagName) {
            message.error('Tag name is required');
            return;
        }

        setIsLoading(true);

        try {
            const createTagResult = await createTagMutation({
                variables: {
                    input: {
                        id: tagName.trim(),
                        name: tagName.trim(),
                        description: tagDescription,
                    },
                },
            });

            const newTagUrn = createTagResult.data?.createTag;

            if (!newTagUrn) {
                message.error('Failed to create tag. An unexpected error occurred');
                setIsLoading(false);
                return;
            }

            if (tagColor) {
                await setTagColorMutation({
                    variables: {
                        urn: newTagUrn,
                        colorHex: tagColor,
                    },
                });
            }

            if (selectedOwnerUrns.length > 0) {
                const ownerInputs = createOwnerInputs(selectedOwnerUrns);
                await batchAddOwnersMutation({
                    variables: {
                        input: {
                            owners: ownerInputs,
                            resources: [{ resourceUrn: newTagUrn }],
                        },
                    },
                });
            }

            message.success(`Tag "${tagName}" successfully created`);
            onClose();
            setTagName('');
            setTagDescription('');
            setTagColor(defaultTagColor);
            setSelectedOwnerUrns([]);
        } catch (e: any) {
            message.destroy();
            message.error(`Failed to create tag. ${e.message}`);
        } finally {
            setIsLoading(false);
        }
    };

    useEnterKeyListener({
        querySelectorToExecuteClick: '#createNewTagButton',
    });

    const buttons: ModalButton[] = [
        {
            text: 'Cancel',
            color: 'violet',
            variant: 'text',
            onClick: onClose,
            buttonDataTestId: 'create-tag-modal-cancel-button',
        },
        {
            text: 'Create',
            id: 'createNewTagButton',
            color: 'violet',
            variant: 'filled',
            onClick: onOk,
            disabled: !tagName || isLoading,
            isLoading,
            buttonDataTestId: 'create-tag-modal-create-button',
        },
    ];

    return (
        <Modal
            title="Create New Tag"
            onCancel={onClose}
            buttons={buttons}
            open={open}
            centered
            width={500}
            dataTestId="create-tag-modal"
        >
            <TagDetailsSection
                tagName={tagName}
                setTagName={setTagName}
                tagDescription={tagDescription}
                setTagDescription={setTagDescription}
                tagColor={tagColor}
                setTagColor={setTagColor}
            />
            <OwnersSection selectedOwnerUrns={selectedOwnerUrns} setSelectedOwnerUrns={setSelectedOwnerUrns} />
        </Modal>
    );
};

export default CreateNewTagModal;
