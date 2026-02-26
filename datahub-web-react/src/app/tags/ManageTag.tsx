import { ColorPicker, Input, Modal } from '@components';
import { message } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
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
                message.error({ content: 'Tag name is required', key: 'tagUpdate', duration: 3 });
                return;
            }

            message.loading({ content: 'Saving changes...', key: 'tagUpdate' });
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
                message.success({ content: 'Tag updated successfully!', key: 'tagUpdate', duration: 2 });
            }

            await refetch();
            onSave?.();
            onClose();
        } catch (error: unknown) {
            const errorMessage = error instanceof Error ? error.message : 'Unknown error';
            message.error({ content: `Failed to update tag: ${errorMessage}`, key: 'tagUpdate', duration: 3 });
        }
    };

    if (loading) {
        return <div>Loading...</div>;
    }

    const buttons: ModalButton[] = [
        { text: 'Cancel', color: 'violet', variant: 'text', onClick: onClose },
        { text: 'Reset', color: 'violet', variant: 'outline', onClick: handleReset, disabled: !hasChanges() },
        {
            text: 'Save',
            color: 'violet',
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
            title="Edit Tag"
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
                        label="Name"
                        value={tagName}
                        setValue={setTagName}
                        placeholder="Enter tag name"
                        isRequired
                        data-testid="tag-name-field"
                    />
                </FormSection>
                <FormSection>
                    <Input
                        label="Description"
                        value={description}
                        setValue={setDescription}
                        placeholder="Tag description"
                        type="textarea"
                        data-testid="tag-description-field"
                    />
                </FormSection>
                <FormSection>
                    <ColorPicker initialColor={colorValue} onChange={handleColorChange} label="Color" />
                </FormSection>
                <OwnersSection selectedOwnerUrns={selectedOwnerUrns} setSelectedOwnerUrns={setSelectedOwnerUrns} />
            </div>
        </Modal>
    );
};

export { ManageTag };
