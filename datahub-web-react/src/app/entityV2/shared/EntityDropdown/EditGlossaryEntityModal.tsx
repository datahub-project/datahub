import { ColorPicker, Editor, Input, Modal, toast } from '@components';
import DOMPurify from 'dompurify';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components/macro';

import { useRefetch } from '@app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import {
    EditorContainer,
    Field,
    useGlossaryNameValidation,
} from '@app/entityV2/shared/EntityDropdown/glossaryEntityModal.shared';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { getParentNodeToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';

import {
    useUpdateDescriptionMutation,
    useUpdateDisplayPropertiesMutation,
    useUpdateNameMutation,
} from '@graphql/mutations.generated';
import { EntityType } from '@types';

interface Props {
    urn: string;
    entityType: EntityType;
    entityData: GenericEntityProperties | null;
    onClose: () => void;
    refetchData?: () => void;
}

function EditGlossaryEntityModal({ urn, entityType, entityData, onClose, refetchData }: Props) {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcf } = useTranslation('common.feedback');
    const { t: tcl } = useTranslation('common.labels');
    const refetch = useRefetch();
    const theme = useTheme();
    const generateGlossaryColor = useGenerateGlossaryColorFromPalette();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate } = useGlossaryEntityData();

    const initialName = entityData?.properties?.name || entityData?.name || '';
    const initialDescription = entityData?.properties?.description || '';
    // Resolve the existing color so the picker opens on "the color you currently see" rather
    // than the gray placeholder. Falls back to the deterministic palette color seeded from
    // the URN — same chain the sidebar/header use.
    const initialColor = entityData?.displayProperties?.colorHex || generateGlossaryColor(urn);

    const [stagedName, setStagedName] = useState<string>(initialName);
    const [nameTouched, setNameTouched] = useState(false);
    const [stagedDocumentation, setStagedDocumentation] = useState<string>(initialDescription);
    const [stagedColor, setStagedColor] = useState<string>(initialColor);
    const [submitting, setSubmitting] = useState(false);

    const [updateName] = useUpdateNameMutation();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [updateDisplayProperties] = useUpdateDisplayPropertiesMutation();

    const entityName =
        entityType === EntityType.GlossaryTerm
            ? t('glossaryTerm', { defaultValue: 'Glossary Term' })
            : t('glossary', { defaultValue: 'Glossary' });

    const nameValidationError = useGlossaryNameValidation(stagedName, entityName);

    const saveDisabled = !!nameValidationError || submitting;

    async function onSave() {
        // Skip mutations whose corresponding field hasn't changed — avoids needless writes
        // and keeps the audit trail clean.
        const trimmedName = stagedName.trim();
        const sanitizedDescription = DOMPurify.sanitize(stagedDocumentation);
        const tasks: Array<Promise<unknown>> = [];

        if (trimmedName !== initialName) {
            tasks.push(updateName({ variables: { input: { name: trimmedName, urn } } }));
        }
        if (sanitizedDescription !== initialDescription) {
            tasks.push(
                updateDescription({
                    variables: { input: { description: sanitizedDescription, resourceUrn: urn } },
                }),
            );
        }
        if (stagedColor !== initialColor) {
            tasks.push(updateDisplayProperties({ variables: { urn, input: { colorHex: stagedColor } } }));
        }

        if (tasks.length === 0) {
            onClose();
            return;
        }

        setSubmitting(true);
        toast.loading(tcf('updating'), { duration: 2 });
        try {
            await Promise.all(tasks);
            toast.success(t('editGlossary.success', { entityName, defaultValue: `Updated ${entityName}!` }), {
                duration: 2,
            });
            refetch();
            if (isInGlossaryContext) {
                // Nudge the glossary browser so the parent's children (and any inherited
                // colors) re-render with the latest values.
                const parentNodeToUpdate = getParentNodeToUpdate(entityData, entityType);
                updateGlossarySidebar([parentNodeToUpdate], urnsToUpdate, setUrnsToUpdate);
            }
            refetchData?.();
            onClose();
        } catch (e) {
            const message = e instanceof Error ? e.message : '';
            toast.error(
                t('editGlossary.error', {
                    errorMessage: message,
                    defaultValue: `Failed to update: \n ${message}`,
                }),
                { duration: 3 },
            );
            setSubmitting(false);
        }
    }

    return (
        <Modal
            title={t('editGlossary.title', { entityName, defaultValue: `Edit ${entityName}` })}
            width={720}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('save'),
                    onClick: onSave,
                    variant: 'filled',
                    disabled: saveDisabled,
                    buttonDataTestId: 'edit-glossary-entity-modal-save-button',
                },
            ]}
            onCancel={onClose}
        >
            <Field data-testid="edit-glossary-entity-modal-name">
                <Input
                    label={tcl('name')}
                    autoFocus
                    placeholder={t('createGlossary.namePlaceholder')}
                    value={stagedName}
                    setValue={(v) => {
                        setStagedName(v);
                        setNameTouched(true);
                    }}
                    isRequired
                    error={nameTouched ? nameValidationError : undefined}
                />
            </Field>
            <Field>
                <EditorContainer>
                    <Editor
                        content={stagedDocumentation}
                        onChange={setStagedDocumentation}
                        placeholder={t('editGlossary.documentationLabel', { defaultValue: 'Documentation' })}
                        dataTestId="edit-glossary-documentation-editor"
                        hideBorder
                    />
                </EditorContainer>
            </Field>
            <Field>
                <ColorPicker
                    label={`${tcl('color')} ${tcl('optional')}`}
                    initialColor={initialColor || theme.colors.colorPickerDefault}
                    onChange={setStagedColor}
                />
            </Field>
        </Modal>
    );
}

export default EditGlossaryEntityModal;
