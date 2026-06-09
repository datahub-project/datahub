import { ColorPicker, Editor, Input, Modal, Text, toast } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import DOMPurify from 'dompurify';
import React, { useEffect, useMemo, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled, { useTheme } from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import NodeParentSelect from '@app/entity/shared/EntityDropdown/NodeParentSelect';
import { getEntityPath } from '@app/entity/shared/containers/profile/utils';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { getGlossaryRootToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { validateCustomUrnId } from '@app/shared/textUtil';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useCreateGlossaryNodeMutation, useCreateGlossaryTermMutation } from '@graphql/glossaryTerm.generated';
import { useUpdateDisplayPropertiesMutation } from '@graphql/mutations.generated';
import { DataHubPageModuleType, EntityType } from '@types';

const Field = styled.div`
    display: flex;
    flex-direction: column;
    gap: 6px;
    margin-bottom: 20px;

    &:last-child {
        margin-bottom: 0;
    }
`;

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

const EditorContainer = styled.div`
    height: 200px;
    overflow: auto;
    border: 1px solid ${(p) => p.theme.colors.border};
    border-radius: 12px;
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
    const theme = useTheme();
    const entityName = entityRegistry.getEntityName(entityType);

    const [stagedId, setStagedId] = useState<string>('');
    const [stagedName, setStagedName] = useState('');
    const [nameTouched, setNameTouched] = useState(false);
    const [idTouched, setIdTouched] = useState(false);
    const [selectedParentUrn, setSelectedParentUrn] = useState<string>(props.isCloning ? '' : entityData.urn);
    const [documentation, setDocumentation] = useState('');
    const [showAdvanced, setShowAdvanced] = useState(false);
    const [selectedColor, setSelectedColor] = useState<string>(theme.colors.colorPickerDefault);
    // Whether the user has explicitly picked a color. If false, we let the backend fall back to
    // the deterministic palette color generated from the URN instead of persisting the default
    // gray placeholder and overriding it.
    const [colorWasPicked, setColorWasPicked] = useState(false);
    const generateGlossaryColor = useGenerateGlossaryColorFromPalette();
    // Tracks the selected parent's effective color so the color picker can pre-fill from it.
    // Mirrors the sidebar/header resolution chain: parent's explicit `displayProperties.colorHex`
    // first, then the deterministic palette color seeded from the parent's urn — otherwise the
    // picker would stay on the default whenever the parent only has a palette-derived color.
    // Once the user explicitly picks a color (`colorWasPicked === true`), changes here no longer
    // move the picker.
    const [parentColor, setParentColor] = useState<string | undefined>(() => {
        if (props.isCloning) return undefined;
        const parent = entityData.entityData;
        if (!parent?.urn) return undefined;
        return parent.displayProperties?.colorHex || generateGlossaryColor(parent.urn);
    });
    const refetch = useRefetch();
    const history = useHistory();
    const { reloadByKeyType } = useReloadableContext();

    const [createGlossaryTermMutation] = useCreateGlossaryTermMutation();
    const [createGlossaryNodeMutation] = useCreateGlossaryNodeMutation();
    const [updateDisplayPropertiesMutation] = useUpdateDisplayPropertiesMutation();

    const showColorPicker = entityType === EntityType.GlossaryNode || entityType === EntityType.GlossaryTerm;

    // Validation rules: matches what the antd Form.Item rules used to enforce.
    const nameValidationError = useMemo<string | undefined>(() => {
        const trimmed = stagedName.trim();
        if (!trimmed) return t('createGlossary.nameRequired', { entityName });
        if (trimmed.length > 100) return t('createGlossary.nameMaxLengthError');
        return undefined;
    }, [stagedName, entityName, t]);

    const idValidationError = useMemo<string | undefined>(() => {
        if (!stagedId) return undefined;
        if (!validateCustomUrnId(stagedId)) return t('createGlossary.idInvalid');
        return undefined;
    }, [stagedId, t]);

    const createButtonDisabled = !!nameValidationError || !!idValidationError;

    useEffect(() => {
        if (props.isCloning && entityData.entityData) {
            const { properties } = entityData.entityData;
            if (properties?.name) setStagedName(properties.name);
            if (properties?.description) setDocumentation(properties.description);
        }
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
                setTimeout(() => {
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
                    if (isInGlossaryContext) {
                        // either refresh this current glossary node or the root nodes or root terms
                        const nodeToUpdate = selectedParentUrn || getGlossaryRootToUpdate(entityType);
                        updateGlossarySidebar([nodeToUpdate], urnsToUpdate, setUrnsToUpdate);
                        if (selectedParentUrn && newEntityUrn) {
                            // Carry the picked color into the optimistic sidebar entry so the new
                            // node renders with the correct color immediately. Without this, the
                            // sidebar falls back to the inherited parent color and only corrects
                            // itself once the search index refetch catches up — which is racy.
                            const optimisticDisplayProperties =
                                showColorPicker && colorWasPicked ? { colorHex: selectedColor } : null;
                            setNodeToNewEntity((currData) => ({
                                ...currData,
                                [selectedParentUrn]: {
                                    urn: newEntityUrn,
                                    type: entityType,
                                    properties: {
                                        name: stagedName,
                                        description: sanitizedDescription || null,
                                    },
                                    displayProperties: optimisticDisplayProperties,
                                },
                            }));
                        }
                    }
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
                    // Reload modules
                    // ChildHierarchy - to update contents module as new term/node could change it
                    reloadByKeyType([
                        getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.ChildHierarchy),
                    ]);
                }, 2000);
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
                    onSelectParent={(parent) =>
                        setParentColor(
                            parent
                                ? parent.displayProperties?.colorHex || generateGlossaryColor(parent.urn)
                                : undefined,
                        )
                    }
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
            {showColorPicker && (
                <Field>
                    <FieldLabel>
                        <Text weight="bold">{tcl('color')}</Text>
                        <OptionalHint>{tcl('optional')}</OptionalHint>
                    </FieldLabel>
                    <ColorPicker
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
