import { Modal } from '@components';
import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';

import { ModalButton } from '@components/components/Modal/Modal';

import OwnersSection from '@app/domainV2/OwnersSection';
import { createOwnerInputs } from '@app/entityV2/shared/utils/selectorUtils';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import TagDetailsSection from '@app/tags/CreateNewTagModal/TagDetailsSection';

import {
    useBatchAddOwnersMutation,
    useBatchAddTagsMutation,
    useSetTagColorMutation,
} from '@graphql/mutations.generated';
import { useCreateTagMutation } from '@graphql/tag.generated';
import { ResourceRefInput } from '@types';

type CreateNewTagModalProps = {
    open: boolean;
    onClose: () => void;
    /**
     * Pre-fills the tag name field. Used by `AddTagsModal` when the user clicks the synthetic
     * "Create <name>" option in the dropdown so they don't have to retype it.
     */
    initialTagName?: string;
    /**
     * When set, the newly-created tag is batch-added to these resources after creation. Mirrors
     * the legacy `CreateTagModal` "create-and-assign" flow used by the entity-sidebar Add Tags
     * button. Omit when the modal is opened from the standalone /tags admin page.
     */
    resources?: ResourceRefInput[];
};

const CreateNewTagModal: React.FC<CreateNewTagModalProps> = ({ onClose, open, initialTagName, resources }) => {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();
    const defaultTagColor = theme.colors.textBrand;
    const [tagName, setTagName] = useState(initialTagName ?? '');
    const [tagDescription, setTagDescription] = useState('');
    const [tagColor, setTagColor] = useState(defaultTagColor);
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [isLoading, setIsLoading] = useState(false);

    const [createTagMutation] = useCreateTagMutation();
    const [setTagColorMutation] = useSetTagColorMutation();
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();
    const [batchAddTagsMutation] = useBatchAddTagsMutation();

    const onOk = async () => {
        if (!tagName) {
            message.error(t('tags.nameRequiredError'));
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
                message.error(t('tags.createError'));
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

            // When launched from `AddTagsModal` the caller passes the entity resources so the new
            // tag is also assigned to them — preserves the legacy inline "create and apply" flow.
            if (resources && resources.length > 0) {
                await batchAddTagsMutation({
                    variables: {
                        input: {
                            tagUrns: [newTagUrn],
                            resources,
                        },
                    },
                });
            }

            message.success(t('tags.createSuccess', { name: tagName }));
            onClose();
            setTagName('');
            setTagDescription('');
            setTagColor(defaultTagColor);
            setSelectedOwnerUrns([]);
        } catch (e: any) {
            message.destroy();
            message.error(t('tags.createErrorDetail', { error: e.message }));
        } finally {
            setIsLoading(false);
        }
    };

    useEnterKeyListener({
        querySelectorToExecuteClick: '#createNewTagButton',
    });

    const buttons: ModalButton[] = [
        {
            text: tc('cancel'),
            color: 'violet',
            variant: 'text',
            onClick: onClose,
            buttonDataTestId: 'create-tag-modal-cancel-button',
        },
        {
            text: tc('create'),
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
            title={t('tags.createModalTitle')}
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
