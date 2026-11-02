import { ColorPicker, Input, Modal, toast } from '@components';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { ModalButton } from '@components/components/Modal/Modal';

import OwnersSection from '@app/domainV2/OwnersSection';
import { createOwnerInputs } from '@app/entityV2/shared/utils/selectorUtils';
import { useBatchAddOwnersMutation, useSetTagColorMutation } from '@src/graphql/mutations.generated';
import { useGetTagQuery, useUpdateTagMutation } from '@src/graphql/tag.generated';

const FormSection = styled.div`
    margin-bottom: 16px;
`;

interface Props {
    tagUrn: string;
    onClose: () => void;
    onSave?: () => void;
    isModalOpen?: boolean;
}

const ManageTag = ({ tagUrn, onClose, onSave, isModalOpen = false }: Props) => {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const { t: tl } = useTranslation('common.labels');
    const theme = useTheme();
    const defaultTagColor = theme.colors.textBrand;
    const { data, loading, refetch } = useGetTagQuery({
        variables: { urn: tagUrn },
        fetchPolicy: 'cache-first',
    });

    const [setTagColorMutation] = useSetTagColorMutation();
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();
    const [updateTagMutation] = useUpdateTagMutation();

    const [colorValue, setColorValue] = useState(defaultTagColor);
    const [originalColor, setOriginalColor] = useState('');
    const [tagName, setTagName] = useState('');
    const [originalTagName, setOriginalTagName] = useState('');
    const [description, setDescription] = useState('');
    const [originalDescription, setOriginalDescription] = useState('');
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [originalOwnerUrns, setOriginalOwnerUrns] = useState<string[]>([]);

    useEffect(() => {
        if (data?.tag) {
            const tagColor = data.tag.properties?.colorHex || defaultTagColor;
            setColorValue(tagColor);
            setOriginalColor(tagColor);

            const tagNameValue = data.tag.properties?.name || data.tag.name || '';
            setTagName(tagNameValue);
            setOriginalTagName(tagNameValue);

            const desc = data.tag.properties?.description || '';
            setDescription(desc);
            setOriginalDescription(desc);

            const existingUrns = (data.tag.ownership?.owners || []).map((o) => o.owner.urn);
            setSelectedOwnerUrns(existingUrns);
            setOriginalOwnerUrns(existingUrns);
        }
    }, [data, defaultTagColor]);

    const handleColorChange = (color: string) => {
        setColorValue(color);
    };

    const newOwnerUrns = useMemo(
        () => selectedOwnerUrns.filter((urn) => !originalOwnerUrns.includes(urn)),
        [selectedOwnerUrns, originalOwnerUrns],
    );

    const hasChanges = () => {
        return (
            tagName !== originalTagName ||
            colorValue !== originalColor ||
            description !== originalDescription ||
            newOwnerUrns.length > 0
        );
    };

    const handleReset = () => {
        setTagName(originalTagName);
        setColorValue(originalColor);
        setDescription(originalDescription);
        setSelectedOwnerUrns(originalOwnerUrns);
    };

    const handleSave = async () => {
        try {
            if (!tagName.trim()) {
                toast.error(t('tags.nameRequiredError'), { duration: 3, key: 'tagUpdate' });
                return;
            }

            toast.loading(t('tags.savingChanges'), { key: 'tagUpdate' });
            let changesMade = false;

            if (colorValue !== originalColor) {
                await setTagColorMutation({ variables: { urn: tagUrn, colorHex: colorValue } });
                changesMade = true;
            }

            if (tagName !== originalTagName || description !== originalDescription) {
                await updateTagMutation({
                    variables: {
                        urn: tagUrn,
                        input: { urn: tagUrn, name: tagName, description: description || undefined },
                    },
                });
                changesMade = true;
            }

            if (newOwnerUrns.length > 0) {
                const ownerInputs = createOwnerInputs(newOwnerUrns);
                await batchAddOwnersMutation({
                    variables: {
                        input: { owners: ownerInputs, resources: [{ resourceUrn: tagUrn }] },
                    },
                });
                changesMade = true;
            }

            if (changesMade) {
                toast.success(t('tags.updateSuccess'), { duration: 2, key: 'tagUpdate' });
            }

            await refetch();
            onSave?.();
            onClose();
        } catch (error: unknown) {
            const errorMessage = error instanceof Error ? error.message : tf('unknownError');
            toast.error(t('tags.updateError', { error: errorMessage }), { duration: 3, key: 'tagUpdate' });
        }
    };

    if (loading) {
        return <div>{tf('loading')}</div>;
    }

    const buttons: ModalButton[] = [
        { text: tc('cancel'), color: 'primary', variant: 'text', onClick: onClose },
        { text: tc('reset'), color: 'primary', variant: 'outline', onClick: handleReset, disabled: !hasChanges() },
        {
            text: tc('save'),
            color: 'primary',
            variant: 'filled',
            onClick: handleSave,
            disabled: !hasChanges() || !tagName.trim(),
            buttonDataTestId: 'update-tag-button',
        },
    ];

    if (!isModalOpen) {
        return null;
    }

    return (
        <Modal
            title={t('tags.editModalTitle')}
            onCancel={onClose}
            buttons={buttons}
            open={isModalOpen}
            centered
            width={400}
            dataTestId="edit-tag-modal"
        >
            <div>
                <FormSection>
                    <Input
                        label={tl('name')}
                        value={tagName}
                        setValue={setTagName}
                        placeholder={t('tags.namePlaceholder')}
                        isRequired
                        data-testid="tag-name-field"
                    />
                </FormSection>
                <FormSection>
                    <Input
                        label={tl('description')}
                        value={description}
                        setValue={setDescription}
                        placeholder={t('tags.descriptionPlaceholderEdit')}
                        type="textarea"
                        data-testid="tag-description-field"
                    />
                </FormSection>
                <FormSection>
                    <ColorPicker initialColor={colorValue} onChange={handleColorChange} label={t('tags.color')} />
                </FormSection>
                <OwnersSection selectedOwnerUrns={selectedOwnerUrns} setSelectedOwnerUrns={setSelectedOwnerUrns} />
            </div>
        </Modal>
    );
};

export { ManageTag };
