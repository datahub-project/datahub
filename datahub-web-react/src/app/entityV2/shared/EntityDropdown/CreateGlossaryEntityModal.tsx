import { ColorPicker, Editor, Input, Modal, Text, toast } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import DOMPurify from 'dompurify';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import NodeParentSelect from '@app/entityV2/shared/EntityDropdown/NodeParentSelect';
import {
    EditorContainer,
    Field,
    useGlossaryNameValidation,
} from '@app/entityV2/shared/EntityDropdown/glossaryEntityModal.shared';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { getGlossaryRootToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { GLOSSARY_SEARCH_INDEX_REFRESH_MS, buildOptimisticGlossaryEntity } from '@app/glossaryV2/utils';
import { validateCustomUrnId } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useCreateGlossaryNodeMutation, useCreateGlossaryTermMutation } from '@graphql/glossaryTerm.generated';
import { useUpdateDisplayPropertiesMutation } from '@graphql/mutations.generated';
import { Entity, EntityType, GlossaryNode } from '@types';

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
    // acryl-main only prop
    canCreateGlossaryEntity: boolean;
    isCloning?: boolean;
}

function CreateGlossaryEntityModal(props: Props) {
    const { entityType, onClose, refetchData, canCreateGlossaryEntity } = props;
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcf } = useTranslation('common.feedback');
    const { t: tcl } = useTranslation('common.labels');
    const entityData = useEntityData();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate, setNodeToNewEntity } = useGlossaryEntityData();
    const entityRegistry = useEntityRegistry();
    const theme = useTheme();

    const [stagedId, setStagedId] = useState<string>('');
    const [stagedName, setStagedName] = useState('');
    const [nameTouched, setNameTouched] = useState(false);
    const [idTouched, setIdTouched] = useState(false);
    const [selectedParentUrn, setSelectedParentUrn] = useState(props.isCloning ? '' : entityData.urn);
    // The hydrated parent entity (or `null` when the user clears the picker, or the form is
    // creating at the root). Tracked alongside `selectedParentUrn` so we can synthesize a
    // correct `parentNodes` chain on the optimistic sidebar entry and inherit the parent's
    // resolved color in the color picker. Initialized lazily from the page entity when the
    // user opens the modal from inside an existing glossary node.
    const [selectedParentEntity, setSelectedParentEntity] = useState<GlossaryNode | null>(() => {
        if (props.isCloning) return null;
        const parent = entityData.entityData;
        if (!parent?.urn) return null;
        return parent as unknown as GlossaryNode;
    });
    const [documentation, setDocumentation] = useState('');
    const [showAdvanced, setShowAdvanced] = useState(false);
    const [selectedColor, setSelectedColor] = useState<string>(theme.colors.colorPickerDefault);
    // Whether the user has explicitly picked a color. If false, we let the backend fall back to
    // the deterministic palette color generated from the URN instead of persisting the default
    // gray placeholder and overriding it.
    const [colorWasPicked, setColorWasPicked] = useState(false);
    const generateGlossaryColor = useGenerateGlossaryColorFromPalette();
    // Effective color of the currently-selected parent, used to pre-fill the color picker.
    // Mirrors the sidebar/header resolution chain: parent's explicit `displayProperties.colorHex`
    // first, then the deterministic palette color seeded from the parent's urn. Once the user
    // explicitly picks a color (`colorWasPicked === true`), changes here no longer move the
    // picker.
    const parentColor = useMemo<string | undefined>(() => {
        if (!selectedParentEntity?.urn) return undefined;
        return selectedParentEntity.displayProperties?.colorHex || generateGlossaryColor(selectedParentEntity.urn);
    }, [selectedParentEntity, generateGlossaryColor]);
    const refetch = useRefetch();

    const [createGlossaryTermMutation] = useCreateGlossaryTermMutation();
    const [createGlossaryNodeMutation] = useCreateGlossaryNodeMutation();
    const [updateDisplayPropertiesMutation] = useUpdateDisplayPropertiesMutation();

    const showColorPicker = entityType === EntityType.GlossaryNode || entityType === EntityType.GlossaryTerm;

    // Show "Glossary" (instead of "Glossary Term Group") in the title/labels when the user is
    // creating a top-level node, since that's the user-facing concept.
    const entityName =
        !selectedParentUrn && entityType === EntityType.GlossaryNode
            ? t('glossary')
            : entityRegistry.getEntityName(entityType);

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
        if (properties?.name) setStagedName(`${properties.name} (copy)`);
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
            .then((result) => {
                toast.loading(tcf('updating'), { duration: 2 });
                const dataKey = entityType === EntityType.GlossaryTerm ? 'createGlossaryTerm' : 'createGlossaryNode';
                const newEntityUrn = result.data?.[dataKey] as string | undefined;
                // Only persist the color if the user actually picked one. Otherwise we'd save the
                // gray placeholder default and override the deterministic palette color the
                // sidebar/header would have generated from the URN. Best-effort follow-up so a
                // color failure doesn't block creation.
                if (showColorPicker && colorWasPicked && newEntityUrn) {
                    updateDisplayPropertiesMutation({
                        variables: {
                            urn: newEntityUrn,
                            input: { colorHex: selectedColor },
                        },
                    }).catch((e) => {
                        console.error('Failed to set glossary color after creation', e);
                    });
                }

                // Push the optimistic sidebar entry immediately so the new node/term appears
                // without waiting for the search index. Keyed by `nodeToUpdate` (parent URN for
                // nested creates, ROOT_NODES / ROOT_TERMS for root creates) so the corresponding
                // sidebar consumer (`useGlossaryChildren` for nested, `GlossaryBrowser` for root)
                // can pick it up. Root creates NEED this because `getRootGlossaryNodes` /
                // `getRootGlossaryTerms` rely on a search index that lags behind the mutation by
                // ~`GLOSSARY_SEARCH_INDEX_REFRESH_MS` — without an optimistic entry the new node
                // is invisible until the index catches up.
                if (isInGlossaryContext && newEntityUrn) {
                    const nodeToUpdate = selectedParentUrn || getGlossaryRootToUpdate(entityType);
                    updateGlossarySidebar([nodeToUpdate], urnsToUpdate, setUrnsToUpdate);
                    const optimistic = buildOptimisticGlossaryEntity({
                        urn: newEntityUrn,
                        entityType,
                        name: stagedName,
                        description: sanitizedDescription || null,
                        colorHex: showColorPicker && colorWasPicked ? selectedColor : undefined,
                        parent: selectedParentUrn ? selectedParentEntity : null,
                    });
                    setNodeToNewEntity((currData) => ({
                        ...currData,
                        [nodeToUpdate]: optimistic as unknown as Entity,
                    }));
                }

                // Defer the analytics event, success toast, and refetch by
                // `GLOSSARY_SEARCH_INDEX_REFRESH_MS` so the refetch sees the new entity in the
                // search index. The optimistic entry above bridges the gap visually until then.
                // Fire-and-forget: the modal closes before this fires, but every callback below
                // targets parent contexts (analytics, refetch, refetchData) that outlive it.
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
                }, GLOSSARY_SEARCH_INDEX_REFRESH_MS);
            })
            .catch((e) => {
                toast.error(t('createGlossary.error', { errorMessage: e.message || '' }), { duration: 3 });
            });
        onClose();
    }

    return (
        <Modal
            title={t('createGlossary.title', { entityName })}
            width={720}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('create'),
                    onClick: createGlossaryEntity,
                    variant: 'filled',
                    disabled: createButtonDisabled || !canCreateGlossaryEntity,
                    buttonDataTestId: 'glossary-entity-modal-create-button',
                },
            ]}
            onCancel={onClose}
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
                <NodeParentSelect
                    label={`${t('parent', { defaultValue: 'Parent' })} ${tcl('optional')}`}
                    selectedParentUrn={selectedParentUrn}
                    setSelectedParentUrn={setSelectedParentUrn}
                    onSelectParent={setSelectedParentEntity}
                />
            </Field>
            <Field>
                <EditorContainer>
                    <Editor
                        content={documentation}
                        onChange={setDocumentation}
                        placeholder={t('createGlossary.addDocumentation')}
                        dataTestId="create-glossary-documentation-editor"
                        hideBorder
                    />
                </EditorContainer>
            </Field>
            {showColorPicker && (
                <Field>
                    <ColorPicker
                        label={`${tcl('color')} ${tcl('optional')}`}
                        // Until the user picks a color, the picker tracks the parent's color
                        // (so a child inherits its group's identity by default). Once they pick,
                        // `colorWasPicked` locks the picker to their choice regardless of any
                        // subsequent parent change.
                        initialColor={colorWasPicked ? selectedColor : parentColor || theme.colors.colorPickerDefault}
                        onChange={(c) => {
                            setSelectedColor(c);
                            setColorWasPicked(true);
                        }}
                    />
                </Field>
            )}
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
