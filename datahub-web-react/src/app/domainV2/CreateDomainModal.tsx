// antd `Form` and `Collapse` are retained because alchemy does not currently provide
// equivalents for `Form` (with field-level rules / Form.useForm) or collapsible panels.
import { toast } from '@components';
import { Collapse, Form } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { Label } from '@components/components/TextArea/components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { UpdatedDomain, useDomainsContext as useDomainsContextV2 } from '@app/domainV2/DomainsContext';
import OwnersSection from '@app/domainV2/OwnersSection';
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
                    // Only persist the color if the user actually picked one. Otherwise we'd
                    // save the gray placeholder default and override the deterministic palette
                    // color the UI would have generated from the URN. Best-effort follow-up so
                    // a color failure doesn't block creation.
                    if (newDomainUrn && colorWasPicked) {
                        updateDisplayPropertiesMutation({
                            variables: {
                                urn: newDomainUrn,
                                input: { colorHex: selectedColor },
                            },
                        }).catch((e) => {
                            console.error('Failed to set domain color after creation', e);
                        });
                    }
                    onCreate?.(
                        newDomainUrn,
                        form.getFieldValue(ID_FIELD_NAME),
                        form.getFieldValue(NAME_FIELD_NAME),
                        form.getFieldValue(DESCRIPTION_FIELD_NAME),
                        selectedParentUrn || undefined,
                    );
                    const newDomain: UpdatedDomain = {
                        urn: newDomainUrn,
                        type: EntityType.Domain,
                        id: form.getFieldValue(ID_FIELD_NAME),
                        properties: {
                            name: form.getFieldValue(NAME_FIELD_NAME),
                            description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
                        },
                        parentDomain: selectedParentUrn || undefined,
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
                {isNestedDomainsEnabled && (
                    <FormItemWithMargin>
                        <Label>{t('create.parentLabel')}</Label>
                        <DomainSelector
                            selectedDomains={selectedParentUrn ? [selectedParentUrn] : []}
                            onDomainsChange={(selectedDomainUrns) => setSelectedParentUrn(selectedDomainUrns[0] || '')}
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
                <Collapse ghost>
                    <Collapse.Panel header={<Label>{t('create.advancedOptions')}</Label>} key="1">
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
                    </Collapse.Panel>
                </Collapse>
            </Form>
        </Modal>
    );
}
