import { toast } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { Label } from '@components/components/TextArea/components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { UpdatedDomain, useDomainsContext as useDomainsContextV2 } from '@app/domainV2/DomainsContext';
import OwnersSection from '@app/domainV2/OwnersSection';
import DomainSelector from '@app/entityV2/shared/DomainSelector/DomainSelector';
import { ChatIconPicker } from '@app/entityV2/shared/containers/profile/header/IconPicker/IconPicker';
import { createOwnerInputs } from '@app/entityV2/shared/utils/selectorUtils';
import { validateCustomUrnId } from '@app/shared/textUtil';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useIsNestedDomainsEnabled } from '@app/useAppConfig';
import { ColorPicker, Input, Modal, TextArea } from '@src/alchemy-components';

import { useCreateDomainMutation } from '@graphql/domain.generated';
import { useUpdateDisplayPropertiesMutation } from '@graphql/mutations.generated';
import { DataHubPageModuleType, EntityType, IconLibrary } from '@types';

const Field = styled.div`
    margin-bottom: 16px;
`;

const AdvancedHeader = styled.button`
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    padding: 8px 0;
    background: transparent;
    border: 0;
    color: ${(p) => p.theme.colors.text};
    cursor: pointer;
    font-size: 14px;

    &:hover {
        color: ${(p) => p.theme.colors.textSecondary};
    }
`;

const AdvancedLabel = styled(Label)`
    margin-bottom: 0;
`;

const AdvancedBody = styled.div`
    padding-top: 4px;
`;

const MODAL_BODY_STYLE: React.CSSProperties = {
    maxHeight: 'calc(90vh - 140px)',
    overflowY: 'auto',
};

type Props = {
    onClose: () => void;
    onCreate?: (
        urn: string,
        id: string | undefined,
        name: string,
        description: string | undefined,
        parentDomain?: string,
    ) => void;
};

export default function CreateDomainModal({ onClose, onCreate }: Props) {
    const { t } = useTranslation('governance.domain');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');
    const isNestedDomainsEnabled = useIsNestedDomainsEnabled();
    const [createDomainMutation] = useCreateDomainMutation();
    const [updateDisplayPropertiesMutation] = useUpdateDisplayPropertiesMutation();
    const { entityData, setNewDomain } = useDomainsContextV2();
    const theme = useTheme();
    const [selectedParentUrn, setSelectedParentUrn] = useState<string>(
        (isNestedDomainsEnabled && entityData?.urn) || '',
    );
    const [name, setName] = useState('');
    const [nameTouched, setNameTouched] = useState(false);
    const [description, setDescription] = useState('');
    const [descriptionTouched, setDescriptionTouched] = useState(false);
    const [customId, setCustomId] = useState('');
    const [idTouched, setIdTouched] = useState(false);
    const [showAdvanced, setShowAdvanced] = useState(false);
    const [selectedColor, setSelectedColor] = useState<string>(theme.colors.colorPickerDefault);
    // Whether the user has explicitly picked a color. If false, we let the backend fall back to
    // the deterministic palette color generated from the URN instead of persisting the default
    // gray placeholder and overriding it.
    const [colorWasPicked, setColorWasPicked] = useState(false);
    const [selectedIcon, setSelectedIcon] = useState<string>('');
    const [iconWasPicked, setIconWasPicked] = useState(false);
    const { loaded: userLoaded, user } = useUserContext();
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [hasInitializedDefaultOwner, setHasInitializedDefaultOwner] = useState(false);

    useEffect(() => {
        if (!hasInitializedDefaultOwner && userLoaded) {
            setSelectedOwnerUrns(user?.urn ? [user.urn] : []);
            setHasInitializedDefaultOwner(true);
        }
    }, [hasInitializedDefaultOwner, user?.urn, userLoaded]);

    const handleSetSelectedOwnerUrns = useCallback((ownerUrns: string[]) => {
        setSelectedOwnerUrns(ownerUrns);
    }, []);

    const { reloadByKeyType } = useReloadableContext();

    const nameError = useMemo(() => {
        const trimmed = name.trim();
        if (!trimmed) return t('create.nameRequired');
        if (trimmed.length > 150) {
            return t('create.nameTooLong', {
                defaultValue: 'Domain name must be 150 characters or fewer.',
            });
        }
        return undefined;
    }, [name, t]);

    // Optional field: empty is fine; reject whitespace-only or overlong values.
    const descriptionError = useMemo(() => {
        if (!description) return undefined;
        if (!description.trim()) {
            return t('create.descriptionInvalid', {
                defaultValue: 'Description cannot be only whitespace.',
            });
        }
        if (description.trim().length > 500) {
            return t('create.descriptionTooLong', {
                defaultValue: 'Description must be 500 characters or fewer.',
            });
        }
        return undefined;
    }, [description, t]);

    const idError = useMemo(() => {
        if (!customId) return undefined;
        if (!validateCustomUrnId(customId)) return t('create.idInvalid');
        return undefined;
    }, [customId, t]);

    const createButtonEnabled = !nameError && !descriptionError && !idError;

    const onCreateDomain = () => {
        setNameTouched(true);
        setDescriptionTouched(true);
        setIdTouched(true);
        if (!createButtonEnabled) return;

        const ownerInputs = createOwnerInputs(selectedOwnerUrns);
        const trimmedName = name.trim();
        const trimmedDescription = description.trim() || undefined;
        const id = customId || undefined;

        createDomainMutation({
            variables: {
                input: {
                    id,
                    name: trimmedName,
                    description: trimmedDescription,
                    parentDomain: selectedParentUrn || undefined,
                    owners: ownerInputs,
                },
            },
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.CreateDomainEvent,
                        parentDomainUrn: selectedParentUrn || undefined,
                    });
                    toast.success(t('create.success'), { duration: 3 });
                    const newDomainUrn = data?.createDomain || '';
                    // Only persist display props the user actually set. Otherwise we'd save the
                    // gray placeholder color and override the deterministic palette from the URN.
                    // Best-effort follow-up so a display-properties failure doesn't block creation.
                    if (newDomainUrn && (colorWasPicked || iconWasPicked)) {
                        updateDisplayPropertiesMutation({
                            variables: {
                                urn: newDomainUrn,
                                input: {
                                    ...(colorWasPicked ? { colorHex: selectedColor } : {}),
                                    ...(iconWasPicked
                                        ? {
                                              icon: {
                                                  iconLibrary: IconLibrary.Phosphor,
                                                  name: selectedIcon,
                                                  style: 'regular',
                                              },
                                          }
                                        : {}),
                                },
                            },
                        }).catch((e) => {
                            console.error('Failed to set domain display properties after creation', e);
                        });
                    }
                    onCreate?.(newDomainUrn, id, trimmedName, trimmedDescription, selectedParentUrn || undefined);
                    const newDomain: UpdatedDomain = {
                        urn: newDomainUrn,
                        type: EntityType.Domain,
                        id: id ?? newDomainUrn,
                        properties: {
                            name: trimmedName,
                            description: trimmedDescription,
                        },
                        // Optimistic sidebar/list icon+color before the follow-up mutation / refetch.
                        displayProperties:
                            colorWasPicked || iconWasPicked
                                ? {
                                      colorHex: colorWasPicked ? selectedColor : null,
                                      icon: iconWasPicked
                                          ? {
                                                name: selectedIcon,
                                                style: 'regular',
                                                iconLibrary: IconLibrary.Phosphor,
                                            }
                                          : null,
                                  }
                                : null,
                        parentDomain: selectedParentUrn || undefined,
                    };
                    setNewDomain(newDomain);
                    // ChildHierarchy - to reload shown child domains on asset summary tab
                    reloadByKeyType(
                        [getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.ChildHierarchy)],
                        3000,
                    );
                }
            })
            .catch((e) => {
                toast.error(t('create.error', { errorMessage: e.message || '' }), { duration: 3 });
            })
            .finally(() => {
                onClose();
            });
    };

    useEnterKeyListener({
        querySelectorToExecuteClick: '#createDomainButton',
    });

    return (
        <Modal
            title={t('create.title')}
            open
            onCancel={onClose}
            bodyStyle={MODAL_BODY_STYLE}
            width={640}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('save'),
                    id: 'createDomainButton',
                    buttonDataTestId: 'create-domain-button',
                    onClick: onCreateDomain,
                    disabled: !createButtonEnabled || !hasInitializedDefaultOwner,
                },
            ]}
        >
            <Field>
                <Input
                    label={tl('name')}
                    data-testid="create-domain-name"
                    placeholder={t('create.namePlaceholder')}
                    value={name}
                    setValue={(v) => {
                        setName(v);
                        setNameTouched(true);
                    }}
                    isRequired
                    error={nameTouched ? nameError : undefined}
                />
            </Field>
            <Field>
                <TextArea
                    label={tl('description')}
                    placeholder={t('create.descriptionPlaceholder')}
                    data-testid="create-domain-description"
                    value={description}
                    onChange={(e) => {
                        setDescription(e.target.value);
                        setDescriptionTouched(true);
                    }}
                    error={descriptionTouched ? descriptionError : undefined}
                />
            </Field>
            <Field>
                <Label>{tl('color')}</Label>
                <ColorPicker
                    initialColor={selectedColor}
                    onChange={(c) => {
                        setSelectedColor(c);
                        setColorWasPicked(true);
                    }}
                />
            </Field>
            <Field>
                <Label>{tl('icon')}</Label>
                <ChatIconPicker
                    color={selectedColor}
                    selectedIcon={selectedIcon || null}
                    onIconPick={(iconName) => {
                        setSelectedIcon(iconName);
                        setIconWasPicked(true);
                    }}
                />
            </Field>
            {isNestedDomainsEnabled && (
                <Field>
                    <Label>{t('create.parentLabel')}</Label>
                    <DomainSelector
                        selectedDomains={selectedParentUrn ? [selectedParentUrn] : []}
                        onDomainsChange={(selectedDomainUrns) => setSelectedParentUrn(selectedDomainUrns[0] || '')}
                        placeholder={t('create.parentPlaceholder')}
                        label=""
                        isMultiSelect={false}
                    />
                </Field>
            )}
            <Field>
                <OwnersSection
                    selectedOwnerUrns={selectedOwnerUrns}
                    setSelectedOwnerUrns={handleSetSelectedOwnerUrns}
                    isDisabled={!hasInitializedDefaultOwner}
                    isLoading={!hasInitializedDefaultOwner}
                />
            </Field>
            <AdvancedHeader type="button" onClick={() => setShowAdvanced((prev) => !prev)}>
                <AdvancedLabel>{t('create.advancedOptions')}</AdvancedLabel>
                {showAdvanced ? <CaretDown size={14} /> : <CaretRight size={14} />}
            </AdvancedHeader>
            {showAdvanced && (
                <AdvancedBody>
                    <Input
                        label={t('create.idLabel')}
                        data-testid="create-domain-id"
                        placeholder={t('create.idPlaceholder')}
                        value={customId}
                        setValue={(v) => {
                            setCustomId(v);
                            setIdTouched(true);
                        }}
                        error={idTouched ? idError : undefined}
                    />
                </AdvancedBody>
            )}
        </Modal>
    );
}
