import { Collapse, Form, message } from 'antd';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

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
import { Input, Modal, TextArea } from '@src/alchemy-components';

import { useCreateDomainMutation } from '@graphql/domain.generated';
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
    const isNestedDomainsEnabled = useIsNestedDomainsEnabled();
    const [createDomainMutation] = useCreateDomainMutation();
    const { entityData, setNewDomain } = useDomainsContextV2();
    const [selectedParentUrn, setSelectedParentUrn] = useState<string>(
        (isNestedDomainsEnabled && entityData?.urn) || '',
    );
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [form] = Form.useForm();
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const { user } = useUserContext();

    // Simply provide current user as placeholder - OwnersSection will handle auto-selection
    const defaultOwners = user ? [user] : [];

    // Stable callback for setting owner URNs
    const handleSetSelectedOwnerUrns = useCallback((ownerUrns: React.SetStateAction<string[]>) => {
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
                    message.success({
                        content: `Created domain!`,
                        duration: 3,
                    });
                    onCreate?.(
                        data?.createDomain || '',
                        form.getFieldValue(ID_FIELD_NAME),
                        form.getFieldValue(NAME_FIELD_NAME),
                        form.getFieldValue(DESCRIPTION_FIELD_NAME),
                        selectedParentUrn || undefined,
                    );
                    const newDomain: UpdatedDomain = {
                        urn: data?.createDomain || '',
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
                message.destroy();
                message.error({ content: `Failed to create Domain!: \n ${e.message || ''}`, duration: 3 });
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
            title="Create New Domain"
            open
            onCancel={onClose}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: 'Save',
                    id: 'createDomainButton',
                    buttonDataTestId: 'create-domain-button',
                    onClick: onCreateDomain,
                    disabled: !createButtonEnabled,
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
                            message: 'Enter a Domain name.',
                        },
                        { whitespace: true },
                        { min: 1, max: 150 },
                    ]}
                    hasFeedback
                >
                    <Input label="Name" data-testid="create-domain-name" placeholder="A name for your domain" />
                </FormItemWithMargin>
                <FormItemWithMargin
                    name={DESCRIPTION_FIELD_NAME}
                    rules={[{ whitespace: true }, { min: 1, max: 500 }]}
                    hasFeedback
                >
                    <TextArea
                        label="Description"
                        placeholder="A description for your domain"
                        data-testid="create-domain-description"
                    />
                </FormItemWithMargin>
                {isNestedDomainsEnabled && (
                    <FormItemWithMargin>
                        <Label>Parent Domains</Label>
                        <DomainSelector
                            selectedDomains={selectedParentUrn ? [selectedParentUrn] : []}
                            onDomainsChange={(selectedDomainUrns) => setSelectedParentUrn(selectedDomainUrns[0] || '')}
                            placeholder="Select parent domain"
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
                        defaultOwners={defaultOwners}
                    />
                </FormItemNoMargin>
                <Collapse ghost>
                    <Collapse.Panel header={<Label>Advanced Options</Label>} key="1">
                        <FormItemWithMargin
                            name={ID_FIELD_NAME}
                            rules={[
                                () => ({
                                    validator(_, value) {
                                        if (value && validateCustomUrnId(value)) {
                                            return Promise.resolve();
                                        }
                                        return Promise.reject(new Error('Please enter a valid Domain id'));
                                    },
                                }),
                            ]}
                        >
                            <Input label="Custom Id" data-testid="create-domain-id" placeholder="engineering" />
                        </FormItemWithMargin>
                    </Collapse.Panel>
                </Collapse>
            </Form>
        </Modal>
    );
}
