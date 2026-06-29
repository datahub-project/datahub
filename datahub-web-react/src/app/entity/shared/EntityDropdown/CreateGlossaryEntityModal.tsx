import { Editor, Input, Modal, Text, toast } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import DOMPurify from 'dompurify';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import NodeParentSelect from '@app/entity/shared/EntityDropdown/NodeParentSelect';
import { getEntityPath } from '@app/entity/shared/containers/profile/utils';
import {
    EditorContainer,
    Field,
    useGlossaryNameValidation,
} from '@app/entityV2/shared/EntityDropdown/glossaryEntityModal.shared';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { getGlossaryRootToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import { GLOSSARY_SEARCH_INDEX_REFRESH_MS, buildOptimisticGlossaryEntity } from '@app/glossaryV2/utils';
import { validateCustomUrnId } from '@app/shared/textUtil';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useCreateGlossaryNodeMutation, useCreateGlossaryTermMutation } from '@graphql/glossaryTerm.generated';
import { DataHubPageModuleType, Entity, EntityType, GlossaryNode } from '@types';

const FieldLabel = styled.div`
    display: flex;
    gap: 6px;
    align-items: baseline;
`;

const OptionalHint = styled(Text).attrs({ type: 'span', weight: 'normal' })`
    color: ${(p) => p.theme.colors.textTertiary};
`;

const HelperText = styled(Text).attrs({ type: 'p' })`
    color: ${(p) => p.theme.colors.textSecondary};
    margin: 0 0 8px 0;
`;

const AdvancedHeader = styled.button`
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 8px 0;
    background: transparent;
    border: 0;
    color: ${(p) => p.theme.colors.textSecondary};
    cursor: pointer;
    font-size: 14px;

    &:hover {
        color: ${(p) => p.theme.colors.text};
    }
`;

const AdvancedBody = styled.div`
    padding-top: 8px;
`;

interface Props {
    entityType: EntityType;
    onClose: () => void;
    refetchData?: () => void;
    isCloning?: boolean;
}

function CreateGlossaryEntityModal(props: Props) {
    const { entityType, onClose, refetchData } = props;
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const { t: tcl } = useTranslation('common.labels');
    const entityData = useEntityData();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate, setNodeToNewEntity } = useGlossaryEntityData();
    const entityRegistry = useEntityRegistry();
    const entityName = entityRegistry.getEntityName(entityType);

    const [stagedId, setStagedId] = useState<string>('');
    const [stagedName, setStagedName] = useState('');
    const [nameTouched, setNameTouched] = useState(false);
    const [idTouched, setIdTouched] = useState(false);
    const [selectedParentUrn, setSelectedParentUrn] = useState<string>(props.isCloning ? '' : entityData.urn);
    // The hydrated parent entity (or `null` when the user clears the picker, or the form is
    // creating at the root). Tracked alongside `selectedParentUrn` so we can synthesize a
    // correct `parentNodes` chain on the optimistic sidebar entry.
    const [selectedParentEntity, setSelectedParentEntity] = useState<GlossaryNode | null>(() => {
        if (props.isCloning) return null;
        const parent = entityData.entityData;
        if (!parent?.urn) return null;
        return parent as unknown as GlossaryNode;
    });
    const [documentation, setDocumentation] = useState('');
    const [showAdvanced, setShowAdvanced] = useState(false);
    const refetch = useRefetch();
    const history = useHistory();
    const { reloadByKeyType } = useReloadableContext();

    const [createGlossaryTermMutation] = useCreateGlossaryTermMutation();
    const [createGlossaryNodeMutation] = useCreateGlossaryNodeMutation();

    const nameValidationError = useGlossaryNameValidation(stagedName, entityName);

    const idValidationError = useMemo<string | undefined>(() => {
        if (!stagedId) return undefined;
        if (!validateCustomUrnId(stagedId)) return t('createGlossary.idInvalid');
        return undefined;
    }, [stagedId, t]);

    const createButtonDisabled = !!nameValidationError || !!idValidationError;

    // Seed the clone form once `entityData.entityData` is hydrated — but only the first time,
    // so a later cache refresh that mutates `entityData.entityData`'s reference doesn't stomp
    // on whatever the user has typed into the name / documentation fields in the meantime.
    const clonePrefillApplied = useRef(false);
    useEffect(() => {
        if (clonePrefillApplied.current) return;
        if (!props.isCloning || !entityData.entityData) return;
        const { properties } = entityData.entityData;
        if (properties?.name) setStagedName(properties.name);
        if (properties?.description) setDocumentation(properties.description);
        clonePrefillApplied.current = true;
    }, [props.isCloning, entityData.entityData]);

    function createGlossaryEntity() {
        const mutation =
            entityType === EntityType.GlossaryTerm ? createGlossaryTermMutation : createGlossaryNodeMutation;

        const sanitizedDescription = DOMPurify.sanitize(documentation);
        mutation({
            variables: {
                input: {
                    id: stagedId.length ? stagedId : undefined,
                    name: stagedName,
                    parentNode: selectedParentUrn || null,
                    description: sanitizedDescription || null,
                },
            },
        })
            .then((res) => {
                toast.loading(tf('updating'), { duration: 2 });
                const dataKey = entityType === EntityType.GlossaryTerm ? 'createGlossaryTerm' : 'createGlossaryNode';
                const newEntityUrn = res.data?.[dataKey] as string | undefined;

                // Push the optimistic sidebar entry immediately — same chain as V2. Includes the
                // root case (`!selectedParentUrn`) so the new root node/term shows up in the
                // sidebar without waiting for the search index.
                if (isInGlossaryContext && newEntityUrn) {
                    const nodeToUpdate = selectedParentUrn || getGlossaryRootToUpdate(entityType);
                    updateGlossarySidebar([nodeToUpdate], urnsToUpdate, setUrnsToUpdate);
                    const optimistic = buildOptimisticGlossaryEntity({
                        urn: newEntityUrn,
                        entityType,
                        name: stagedName,
                        description: sanitizedDescription || null,
                        parent: selectedParentUrn ? selectedParentEntity : null,
                    });
                    setNodeToNewEntity((currData) => ({
                        ...currData,
                        [nodeToUpdate]: optimistic as unknown as Entity,
                    }));
                }

                // Defer the analytics event, success toast, refetch, and clone-redirect by
                // `GLOSSARY_SEARCH_INDEX_REFRESH_MS` so the refetch sees the new entity in the
                // search index. Fire-and-forget — every callback below targets parent contexts
                // (analytics, refetch, history) that outlive the modal.
                window.setTimeout(() => {
                    analytics.event({
                        type: EventType.CreateGlossaryEntityEvent,
                        entityType,
                        parentNodeUrn: selectedParentUrn || undefined,
                    });
                    toast.success(
                        t('createGlossary.success', {
                            entityName: entityRegistry.getEntityName(entityType),
                        }),
                        { duration: 2 },
                    );
                    refetch();
                    if (refetchData) {
                        refetchData();
                    }
                    if (props.isCloning) {
                        const redirectUrn =
                            entityType === EntityType.GlossaryTerm
                                ? res.data?.createGlossaryTerm
                                : res.data?.createGlossaryNode;
                        history.push(getEntityPath(entityType, redirectUrn, entityRegistry, false, false));
                    }
                    // ChildHierarchy module needs to refresh since a new term/node can change the
                    // contents shown on the parent's hierarchy view.
                    reloadByKeyType([
                        getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.ChildHierarchy),
                    ]);
                }, GLOSSARY_SEARCH_INDEX_REFRESH_MS);
            })
            .catch((e) => {
                toast.error(t('createGlossary.error', { errorMessage: e.message || '' }), { duration: 3 });
            });
        onClose();
    }

    return (
        <Modal
            title={t('createGlossary.title', { entityName: entityRegistry.getEntityName(entityType) })}
            open
            onCancel={onClose}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('create'),
                    variant: 'filled',
                    disabled: createButtonDisabled,
                    onClick: createGlossaryEntity,
                    buttonDataTestId: 'glossary-entity-modal-create-button',
                },
            ]}
        >
            <Field data-testid="create-glossary-entity-modal-name">
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
                <FieldLabel>
                    <Text weight="bold">
                        <Trans t={t} i18nKey="createGlossary.parentLabel" components={{ optional: <OptionalHint /> }} />
                    </Text>
                </FieldLabel>
                <NodeParentSelect
                    selectedParentUrn={selectedParentUrn}
                    setSelectedParentUrn={setSelectedParentUrn}
                    onSelectParent={setSelectedParentEntity}
                />
            </Field>
            <Field>
                <FieldLabel>
                    <Text weight="bold">
                        <Trans
                            t={t}
                            i18nKey="createGlossary.documentationLabel"
                            components={{ optional: <OptionalHint /> }}
                        />
                    </Text>
                </FieldLabel>
                <EditorContainer>
                    <Editor
                        content={documentation}
                        onChange={setDocumentation}
                        dataTestId="create-glossary-documentation-editor"
                        hideBorder
                    />
                </EditorContainer>
            </Field>
            <AdvancedHeader type="button" onClick={() => setShowAdvanced((prev) => !prev)}>
                {showAdvanced ? <CaretDown size={14} /> : <CaretRight size={14} />}
                {t('createGlossary.advanced')}
            </AdvancedHeader>
            {showAdvanced && (
                <AdvancedBody>
                    <Field>
                        <HelperText>{t('createGlossary.idHelp')}</HelperText>
                        <Input
                            label={t('createGlossary.idLabel', {
                                entityName: entityRegistry.getEntityName(props.entityType),
                            })}
                            placeholder={t('createGlossary.idPlaceholder')}
                            value={stagedId}
                            setValue={(v) => {
                                setStagedId(v);
                                setIdTouched(true);
                            }}
                            error={idTouched ? idValidationError : undefined}
                        />
                    </Field>
                </AdvancedBody>
            )}
        </Modal>
    );
}

export default CreateGlossaryEntityModal;
