// antd `Form` is retained because alchemy does not currently provide an equivalent
// (field-level rules / `Form.useForm`).
import { toast } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Form } from 'antd';
import React, { Suspense, lazy, useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { Label } from '@components/components/TextArea/components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { UpdatedDomain, useDomainsContext as useDomainsContextV2 } from '@app/domainV2/DomainsContext';
import OwnersSection from '@app/domainV2/OwnersSection';
import {
    buildDomainDisplayInput,
    buildOptimisticDomainDisplayProperties,
} from '@app/entityV2/domain/utils/displayProperties';
import DomainSelector from '@app/entityV2/shared/DomainSelector/DomainSelector';
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
import { DataHubPageModuleType, EntityType } from '@types';

// Shares one lazy chunk with the edit-domain flow — Vite dedupes the same import specifier.
// See EditDomainModal / IconPicker for the rationale on the single-chunk static-import layout.
const ChatIconPicker = lazy(() =>
    import('@app/entityV2/shared/containers/profile/header/IconPicker/IconPicker').then((mod) => ({
        default: mod.ChatIconPicker,
    })),
);

const FormItem = styled(Form.Item)`
    .ant-form-item-label {
        padding-bottom: 2px;
    }
`;

const FormItemWithMargin = styled(FormItem)`
    margin-bottom: 16px;
`;

const FormItemNoMargin = styled(FormItem)`
    margin-bottom: 0px;
`;

// Match the AdvancedHeader / AdvancedBody styling used by CreateGlossaryEntityModal
// so both create flows share the same "Advanced Options" disclosure. Only the create
// flow needs this — EditDomainModal no longer has an Advanced section since its former
// contents (icon picker) were moved inline.
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

// Cap body height so the whole modal (body + ~120px of header/footer chrome) stays around
// 80vh even when the icon picker is visible. Content that overflows scrolls in-place —
// matches the pattern used in PolicyBuilderModal / QueryModal.
const ScrollableBody = styled.div`
    max-height: 65vh;
    overflow-y: auto;
`;

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

const ID_FIELD_NAME = 'id';
const NAME_FIELD_NAME = 'name';
const DESCRIPTION_FIELD_NAME = 'description';

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
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [selectedColor, setSelectedColor] = useState<string>(theme.colors.colorPickerDefault);
    // Whether the user has explicitly picked a color. If false, we let the backend fall back to
    // the deterministic palette color generated from the URN instead of persisting the default
    // gray placeholder and overriding it.
    const [colorWasPicked, setColorWasPicked] = useState(false);
    // Empty string == user did not pick an icon → letter avatar (initial of the domain name).
    // Only persisted after successful creation, and only when non-empty (see onCreateDomain).
    const [stagedIconName, setStagedIconName] = useState<string>('');
    const [showAdvanced, setShowAdvanced] = useState<boolean>(false);
    const [form] = Form.useForm();
    const { loaded: userLoaded, user } = useUserContext();
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [hasInitializedDefaultOwner, setHasInitializedDefaultOwner] = useState(false);

    useEffect(() => {
        if (!hasInitializedDefaultOwner && userLoaded) {
            setSelectedOwnerUrns(user?.urn ? [user.urn] : []);
            setHasInitializedDefaultOwner(true);
        }
    }, [hasInitializedDefaultOwner, user?.urn, userLoaded]);

    // Stable callback for setting owner URNs
    const handleSetSelectedOwnerUrns = useCallback((ownerUrns: string[]) => {
        setSelectedOwnerUrns(ownerUrns);
    }, []);

    const { reloadByKeyType } = useReloadableContext();

    const onCreateDomain = () => {
        // Create owner input objects from selected owner URNs using utility
        const ownerInputs = createOwnerInputs(selectedOwnerUrns);

        createDomainMutation({
            variables: {
                input: {
                    id: form.getFieldValue(ID_FIELD_NAME),
                    name: form.getFieldValue(NAME_FIELD_NAME),
                    description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
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
                    // Only persist display properties the user actually set. Skipping the color
                    // when unset lets the backend fall back to the deterministic palette color
                    // generated from the URN; skipping the icon leaves the domain rendering the
                    // letter avatar. Best-effort follow-up so a display-properties failure
                    // doesn't block domain creation itself.
                    const displayInput = buildDomainDisplayInput({
                        colorHex: colorWasPicked ? selectedColor : undefined,
                        iconName: stagedIconName || undefined,
                    });
                    if (newDomainUrn && displayInput) {
                        updateDisplayPropertiesMutation({
                            variables: { urn: newDomainUrn, input: displayInput },
                        }).catch((e) => {
                            console.error('Failed to set domain display properties after creation', e);
                        });
                    }
                    onCreate?.(
                        newDomainUrn,
                        form.getFieldValue(ID_FIELD_NAME),
                        form.getFieldValue(NAME_FIELD_NAME),
                        form.getFieldValue(DESCRIPTION_FIELD_NAME),
                        selectedParentUrn || undefined,
                    );
                    // Populate `displayProperties` on the optimistic sidebar entry with the same
                    // values we just sent to `updateDisplayProperties`. Without this the sidebar
                    // shows the letter-avatar + palette-from-URN fallback until a full page
                    // refresh — the picked color and icon don't propagate because the create
                    // mutation itself returns only the URN, so Apollo can't normalize the aspect
                    // into the sidebar's `listDomains` cache.
                    const newDomain: UpdatedDomain = {
                        urn: newDomainUrn,
                        type: EntityType.Domain,
                        id: form.getFieldValue(ID_FIELD_NAME),
                        properties: {
                            name: form.getFieldValue(NAME_FIELD_NAME),
                            description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
                        },
                        parentDomain: selectedParentUrn || undefined,
                        displayProperties: buildOptimisticDomainDisplayProperties({
                            colorHex: colorWasPicked ? selectedColor : undefined,
                            iconName: stagedIconName || undefined,
                        }),
                    };
                    setNewDomain(newDomain);
                    form.resetFields();
                    // Reload modules
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

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createDomainButton',
    });

    return (
        <Modal
            title={t('create.title')}
            open
            onCancel={onClose}
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
            <ScrollableBody>
                <Form
                    form={form}
                    initialValues={{}}
                    layout="vertical"
                    onFieldsChange={() => {
                        setCreateButtonEnabled(!form.getFieldsError().some((field) => field.errors.length > 0));
                    }}
                >
                    <FormItemWithMargin
                        name={NAME_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: t('create.nameRequired'),
                            },
                            { whitespace: true },
                            { min: 1, max: 150 },
                        ]}
                        hasFeedback
                    >
                        <Input
                            label={tl('name')}
                            data-testid="create-domain-name"
                            placeholder={t('create.namePlaceholder')}
                        />
                    </FormItemWithMargin>
                    <FormItemWithMargin
                        name={DESCRIPTION_FIELD_NAME}
                        rules={[{ whitespace: true }, { min: 1, max: 500 }]}
                        hasFeedback
                    >
                        <TextArea
                            label={tl('description')}
                            placeholder={t('create.descriptionPlaceholder')}
                            data-testid="create-domain-description"
                        />
                    </FormItemWithMargin>
                    <FormItemWithMargin>
                        <Label>{tl('color')}</Label>
                        <ColorPicker
                            initialColor={selectedColor}
                            onChange={(c) => {
                                setSelectedColor(c);
                                setColorWasPicked(true);
                            }}
                        />
                    </FormItemWithMargin>
                    <FormItemWithMargin>
                        <Label>{`${tl('icon')} ${tl('optional')}`}</Label>
                        <Suspense fallback={null}>
                            <ChatIconPicker
                                color={colorWasPicked ? selectedColor : undefined}
                                onIconPick={setStagedIconName}
                                selectedIcon={stagedIconName}
                            />
                        </Suspense>
                    </FormItemWithMargin>
                    {isNestedDomainsEnabled && (
                        <FormItemWithMargin>
                            <Label>{t('create.parentLabel')}</Label>
                            <DomainSelector
                                selectedDomains={selectedParentUrn ? [selectedParentUrn] : []}
                                onDomainsChange={(selectedDomainUrns) =>
                                    setSelectedParentUrn(selectedDomainUrns[0] || '')
                                }
                                placeholder={t('create.parentPlaceholder')}
                                label=""
                                isMultiSelect={false}
                            />
                        </FormItemWithMargin>
                    )}
                    {/* Owners Section */}
                    <FormItemNoMargin>
                        <OwnersSection
                            selectedOwnerUrns={selectedOwnerUrns}
                            setSelectedOwnerUrns={handleSetSelectedOwnerUrns}
                            isDisabled={!hasInitializedDefaultOwner}
                            isLoading={!hasInitializedDefaultOwner}
                        />
                    </FormItemNoMargin>
                    <AdvancedHeader type="button" onClick={() => setShowAdvanced((prev) => !prev)}>
                        {showAdvanced ? <CaretDown size={14} /> : <CaretRight size={14} />}
                        {t('create.advancedOptions')}
                    </AdvancedHeader>
                    {showAdvanced && (
                        <AdvancedBody>
                            <FormItemWithMargin
                                name={ID_FIELD_NAME}
                                rules={[
                                    () => ({
                                        validator(_, value) {
                                            if (value && validateCustomUrnId(value)) {
                                                return Promise.resolve();
                                            }
                                            return Promise.reject(new Error(t('create.idInvalid')));
                                        },
                                    }),
                                ]}
                            >
                                <Input
                                    label={t('create.idLabel')}
                                    data-testid="create-domain-id"
                                    placeholder={t('create.idPlaceholder')}
                                />
                            </FormItemWithMargin>
                        </AdvancedBody>
                    )}
                </Form>
            </ScrollableBody>
        </Modal>
    );
}
