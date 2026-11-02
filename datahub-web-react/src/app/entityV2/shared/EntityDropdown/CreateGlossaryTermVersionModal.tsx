import { Input, Modal, Text, toast } from '@components';
import DOMPurify from 'dompurify';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { v4 as uuidv4 } from 'uuid';

import { Editor } from '@components/components/Editor';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { EditorContainer } from '@app/entityV2/shared/EntityDropdown/glossaryEntityModal.shared';
import { suggestNextVersionLabel } from '@app/entityV2/shared/EntityDropdown/utils';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { buildOptimisticGlossaryEntity, getParentNodeToUpdate, updateGlossarySidebar } from '@app/glossaryV2/utils';
import { validateCustomUrnId } from '@app/sharedV2/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useCreateGlossaryTermVersionMutation } from '@graphql/glossaryTermVersion.generated';
import { Entity, EntityType, GlossaryNode } from '@types';

const Body = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const AdvancedToggle = styled.button`
    align-self: flex-start;
    background: none;
    border: none;
    padding: 0;
    cursor: pointer;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const ErrorText = styled.div`
    color: ${(props) => props.theme.colors.textError};
`;

type Props = {
    termUrn: string;
    currentName: string;
    currentDefinition?: string | null;
    currentVersionLabel?: string | null;
    onClose: () => void;
};

export default function CreateGlossaryTermVersionModal({
    termUrn,
    currentName,
    currentDefinition,
    currentVersionLabel,
    onClose,
}: Props) {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { entityData } = useEntityData();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate, setNodeToNewEntity } = useGlossaryEntityData();
    const [createVersion] = useCreateGlossaryTermVersionMutation({
        // Drop the previous term's cached profile: it may have just become v1, and its version
        // properties would otherwise stay stale when the user navigates back to it.
        update: (cache) => {
            cache.evict({ id: 'ROOT_QUERY', fieldName: 'glossaryTerm', args: { urn: termUrn } });
        },
    });

    const [name, setName] = useState(currentName);
    const [definition, setDefinition] = useState(currentDefinition ?? '');
    const [versionLabel, setVersionLabel] = useState(suggestNextVersionLabel(currentVersionLabel));
    const [comment, setComment] = useState('');
    const [customId, setCustomId] = useState('');
    const [showAdvanced, setShowAdvanced] = useState(false);
    const [error, setError] = useState<string | undefined>();
    const [isSubmitting, setIsSubmitting] = useState(false);

    const idIsValid = !customId.trim() || validateCustomUrnId(customId.trim());
    const canSubmit = !!name.trim() && !!versionLabel.trim() && idIsValid && !isSubmitting;

    const submit = async () => {
        // The mutation returns only the version set, and the new draft is not its latest version, so
        // the id is chosen here to know which page to open afterwards.
        const id = customId.trim() || uuidv4();
        const newTermUrn = `urn:li:glossaryTerm:${id}`;
        const description = DOMPurify.sanitize(definition);
        setIsSubmitting(true);
        setError(undefined);
        try {
            await createVersion({
                variables: {
                    input: {
                        previousTermUrn: termUrn,
                        name: name.trim(),
                        description,
                        version: versionLabel.trim(),
                        comment: comment.trim() || undefined,
                        id,
                    },
                },
            });
            // The new version sits next to the previous one; add it to the sidebar optimistically,
            // as the search index lags behind the mutation.
            if (isInGlossaryContext) {
                const nodeToUpdate = getParentNodeToUpdate(entityData, EntityType.GlossaryTerm);
                updateGlossarySidebar([nodeToUpdate], urnsToUpdate, setUrnsToUpdate);
                const optimistic = buildOptimisticGlossaryEntity({
                    urn: newTermUrn,
                    entityType: EntityType.GlossaryTerm,
                    name: name.trim(),
                    description: description || null,
                    parent: (entityData?.parentNodes?.nodes?.[0] as GlossaryNode | undefined) ?? null,
                });
                setNodeToNewEntity((curr) => ({ ...curr, [nodeToUpdate]: optimistic as unknown as Entity }));
            }
            toast.success(t('newVersion.success'));
            onClose();
            history.push(entityRegistry.getEntityUrl(EntityType.GlossaryTerm, newTermUrn));
        } catch (e) {
            setError(e instanceof Error ? e.message : String(e));
        } finally {
            setIsSubmitting(false);
        }
    };

    return (
        <Modal
            title={t('newVersion.modalTitle')}
            onCancel={onClose}
            buttons={[
                { text: tc('cancel'), variant: 'text', onClick: onClose, key: 'cancel' },
                {
                    text: tc('create'),
                    variant: 'filled',
                    onClick: submit,
                    key: 'submit',
                    disabled: !canSubmit,
                    buttonDataTestId: 'new-version-submit',
                },
            ]}
        >
            <Body>
                <Text>{currentVersionLabel ? t('newVersion.draftNote') : t('newVersion.firstVersionNote')}</Text>
                <Input label={t('newVersion.name')} value={name} setValue={setName} isRequired />
                <Input
                    label={t('newVersion.versionLabel')}
                    placeholder={t('newVersion.versionLabelPlaceholder')}
                    value={versionLabel}
                    setValue={setVersionLabel}
                    isRequired
                    inputTestId="new-version-label"
                />
                <div>
                    <Text weight="semiBold">{t('newVersion.definition')}</Text>
                    <EditorContainer>
                        <Editor content={definition} onChange={setDefinition} hideBorder />
                    </EditorContainer>
                </div>
                <Input
                    label={t('newVersion.comment')}
                    placeholder={t('newVersion.commentPlaceholder')}
                    value={comment}
                    setValue={setComment}
                />
                <AdvancedToggle
                    type="button"
                    onClick={() => setShowAdvanced((prev) => !prev)}
                    data-testid="new-version-advanced"
                >
                    {t('newVersion.advanced')}
                </AdvancedToggle>
                {showAdvanced && (
                    <Input
                        label={t('newVersion.customId')}
                        value={customId}
                        setValue={setCustomId}
                        error={idIsValid ? undefined : t('newVersion.customIdInvalid')}
                        inputTestId="new-version-custom-id"
                    />
                )}
                {error && <ErrorText>{error}</ErrorText>}
            </Body>
        </Modal>
    );
}
