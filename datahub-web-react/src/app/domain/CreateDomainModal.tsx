import React, { useState } from 'react';
import styled from 'styled-components';
import { message, Button, Input, Modal, Typography, Form, Collapse, Tag } from 'antd';
import { useTranslation } from 'react-i18next';
import { useCreateDomainMutation } from '../../graphql/domain.generated';
import { useEnterKeyListener } from '../shared/useEnterKeyListener';
import { validateCustomUrnId } from '../shared/textUtil';
import analytics, { EventType } from '../analytics';
import DomainParentSelect from '../entity/shared/EntityDropdown/DomainParentSelect';
import { useIsNestedDomainsEnabled } from '../useAppConfig';
import { useDomainsContext } from './DomainsContext';

const SuggestedNamesGroup = styled.div`
    margin-top: 8px;
`;

const ClickableTag = styled(Tag)`
    :hover {
        cursor: pointer;
    }
`;

const FormItem = styled(Form.Item)`
    .ant-form-item-label {
        padding-bottom: 2px;
    }
`;

const FormItemWithMargin = styled(FormItem)`
    margin-bottom: 16px;
`;

const FormItemNoMargin = styled(FormItem)`
    margin-bottom: 0;
`;

const FormItemLabel = styled(Typography.Text)`
    font-weight: 600;
    color: #373d44;
`;

const AdvancedLabel = styled(Typography.Text)`
    color: #373d44;
`;

type Props = {
    onClose: () => void;
    onCreate: (
        urn: string,
        id: string | undefined,
        name: string,
        description: string | undefined,
        parentDomain?: string,
    ) => void;
};

const SUGGESTED_DOMAIN_NAMES = ['Engineering', 'Marketing', 'Sales', 'Product'];

const ID_FIELD_NAME = 'id';
const NAME_FIELD_NAME = 'name';
const DESCRIPTION_FIELD_NAME = 'description';

export default function CreateDomainModal({ onClose, onCreate }: Props) {
    const { t } = useTranslation();
    const isNestedDomainsEnabled = useIsNestedDomainsEnabled();
    const [createDomainMutation] = useCreateDomainMutation();
    const { entityData } = useDomainsContext();
    const [selectedParentUrn, setSelectedParentUrn] = useState<string>(
        (isNestedDomainsEnabled && entityData?.urn) || '',
    );
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [form] = Form.useForm();

    const onCreateDomain = () => {
        createDomainMutation({
            variables: {
                input: {
                    id: form.getFieldValue(ID_FIELD_NAME),
                    name: form.getFieldValue(NAME_FIELD_NAME),
                    description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
                    parentDomain: selectedParentUrn || undefined,
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
                        content: t('crud.success.createdDomain'),
                        duration: 3,
                    });
                    onCreate(
                        data?.createDomain || '',
                        form.getFieldValue(ID_FIELD_NAME),
                        form.getFieldValue(NAME_FIELD_NAME),
                        form.getFieldValue(DESCRIPTION_FIELD_NAME),
                        selectedParentUrn || undefined,
                    );
                    form.resetFields();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `${t('crud.error.failedToCreateDomain')}: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
        onClose();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createDomainButton',
    });

    return (
        <Modal
            title={t('ingest.createNewDomain')}
            visible
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        {t('common.cancel')}
                    </Button>
                    <Button
                        id="createDomainButton"
                        data-testid="create-domain-button"
                        onClick={onCreateDomain}
                        disabled={!createButtonEnabled}
                    >
                        {t('common.create')}
                    </Button>
                </>
            }
        >
            <Form
                form={form}
                initialValues={{}}
                layout="vertical"
                onFieldsChange={() => {
                    setCreateButtonEnabled(!form.getFieldsError().some((field) => field.errors.length > 0));
                }}
            >
                {isNestedDomainsEnabled && (
                    <FormItemWithMargin
                        label={
                            <FormItemLabel>
                                {t('common.parent')} {t('common.optional')}
                            </FormItemLabel>
                        }
                    >
                        <DomainParentSelect
                            selectedParentUrn={selectedParentUrn}
                            setSelectedParentUrn={setSelectedParentUrn}
                        />
                    </FormItemWithMargin>
                )}
                <FormItemWithMargin label={<FormItemLabel>{t('common.name')}</FormItemLabel>}>
                    <FormItemNoMargin
                        name={NAME_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: t('form.enterDomainName'),
                            },
                            { whitespace: true },
                            { min: 1, max: 150 },
                        ]}
                        hasFeedback
                    >
                        <Input data-testid="create-domain-name" placeholder={t('placeholder.domainName')} />
                    </FormItemNoMargin>
                    <SuggestedNamesGroup>
                        {SUGGESTED_DOMAIN_NAMES.map((name) => {
                            return (
                                <ClickableTag
                                    key={name}
                                    onClick={() => {
                                        form.setFieldsValue({
                                            name,
                                        });
                                        setCreateButtonEnabled(true);
                                    }}
                                >
                                    {name}
                                </ClickableTag>
                            );
                        })}
                    </SuggestedNamesGroup>
                </FormItemWithMargin>
                <FormItemWithMargin
                    label={<FormItemLabel>{t('common.description')}</FormItemLabel>}
                    help={t('common.descriptionHelp')}
                >
                    <FormItemNoMargin
                        name={DESCRIPTION_FIELD_NAME}
                        rules={[{ whitespace: true }, { min: 1, max: 500 }]}
                        hasFeedback
                    >
                        <Input.TextArea
                            placeholder={t('placeholder.domainDescription')}
                            data-testid="create-domain-description"
                        />
                    </FormItemNoMargin>
                </FormItemWithMargin>
                <Collapse ghost>
                    <Collapse.Panel header={<AdvancedLabel>{t('common.advancedOptions')}</AdvancedLabel>} key="1">
                        <FormItemWithMargin
                            label={<Typography.Text strong>{t('onBoarding.domains.idDomain')}</Typography.Text>}
                            help={t('group.groupIdDescription')}
                        >
                            <FormItemNoMargin
                                name={ID_FIELD_NAME}
                                rules={[
                                    () => ({
                                        validator(_, value) {
                                            if (value && validateCustomUrnId(value)) {
                                                return Promise.resolve();
                                            }
                                            return Promise.reject(new Error(t('form.enterValidDomainId')));
                                        },
                                    }),
                                ]}
                            >
                                <Input data-testid="create-domain-id" placeholder="engineering" />
                            </FormItemNoMargin>
                        </FormItemWithMargin>
                    </Collapse.Panel>
                </Collapse>
            </Form>
        </Modal>
    );
}
