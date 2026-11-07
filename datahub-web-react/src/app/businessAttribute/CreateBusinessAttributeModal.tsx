import { Button, CollapsiblePanel, Modal, Text, toast } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Form, Input, Select } from 'antd';
import DOMPurify from 'dompurify';
import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { SchemaFieldDataType } from '@app/businessAttribute/businessAttributeUtils';
import DescriptionModal from '@app/entityV2/shared/components/legacy/DescriptionModal';
import { validateCustomUrnId } from '@app/sharedV2/textUtil';
import { useEnterKeyListener } from '@app/sharedV2/useEnterKeyListener';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useCreateBusinessAttributeMutation } from '@graphql/businessAttribute.generated';
import { CreateBusinessAttributeInput, EntityType } from '@types';

type Props = {
    open: boolean;
    onClose: () => void;
    onCreateBusinessAttribute: () => void;
};

type FormProps = {
    name: string;
    description?: string;
    dataType?: SchemaFieldDataType;
};

const DataTypeSelectContainer = styled.div`
    padding: 1px;
`;

const DataTypeSelect = styled(Select)`
    && {
        width: 100%;
        margin-top: 1em;
        margin-bottom: 1em;
    }
`;

const StyledItem = styled(Form.Item)`
    margin-bottom: 0;
`;

const OptionalWrapper = styled.span`
    font-weight: normal;
`;

const StyledButton = styled(Button)`
    && {
        padding: 0;
    }
`;

const HelpText = styled(Text)`
    && {
        margin-bottom: 1em;
    }
`;

// Ensures that any newly added datatype is automatically included in the user dropdown.
const DATA_TYPES = Object.values(SchemaFieldDataType);

export default function CreateBusinessAttributeModal({ open, onClose, onCreateBusinessAttribute }: Props) {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const { t: tl } = useTranslation('common.labels');
    const [createButtonEnabled, setCreateButtonEnabled] = useState(true);

    const [createBusinessAttribute] = useCreateBusinessAttributeMutation();

    const [isDocumentationModalVisible, setIsDocumentationModalVisible] = useState(false);

    const [documentation, setDocumentation] = useState('');

    const [form] = Form.useForm<FormProps>();

    const entityRegistry = useEntityRegistry();

    const [stagedId, setStagedId] = useState<string | undefined>(undefined);

    // Function to handle the close or cross button of Create Business Attribute Modal
    const onModalClose = () => {
        form.resetFields();
        onClose();
    };

    const onCreateNewBusinessAttribute = () => {
        const { name, dataType } = form.getFieldsValue();
        const sanitizedDescription = DOMPurify.sanitize(documentation);
        const input: CreateBusinessAttributeInput = {
            id: stagedId?.length ? stagedId : undefined,
            name,
            description: sanitizedDescription,
            type: dataType,
        };
        createBusinessAttribute({ variables: { input } })
            .then(() => {
                toast.loading(tf('updating'), { duration: 2 });
                setTimeout(() => {
                    analytics.event({
                        type: EventType.CreateBusinessAttributeEvent,
                        name,
                    });
                    toast.success(
                        t('businessAttribute.createSuccess', {
                            entityName: entityRegistry.getEntityName(EntityType.BusinessAttribute),
                        }),
                        { duration: 2 },
                    );
                    if (onCreateBusinessAttribute) {
                        onCreateBusinessAttribute();
                    }
                }, 2000);
            })
            .catch((e) => {
                toast.destroy();
                toast.error(t('businessAttribute.createError', { error: e.message || '' }), { duration: 3 });
            });
        onModalClose();
        setDocumentation('');
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createBusinessAttributeButton',
    });

    function addDocumentation(description: string) {
        setDocumentation(description);
        setIsDocumentationModalVisible(false);
    }

    return (
        <>
            <Modal
                title={t('businessAttribute.createModalTitle')}
                open={open}
                onCancel={onModalClose}
                buttons={[
                    {
                        text: tc('cancel'),
                        variant: 'text',
                        onClick: onModalClose,
                        buttonDataTestId: 'cancel-create-business-attribute-button',
                    },
                    {
                        text: tc('create'),
                        variant: 'filled',
                        id: 'createBusinessAttributeButton',
                        onClick: onCreateNewBusinessAttribute,
                        disabled: createButtonEnabled,
                        buttonDataTestId: 'create-business-attribute-button',
                    },
                ]}
            >
                <Form
                    form={form}
                    initialValues={{ dataType: DATA_TYPES[2] }}
                    layout="vertical"
                    onFieldsChange={() =>
                        setCreateButtonEnabled(form.getFieldsError().some((field) => field.errors.length > 0))
                    }
                >
                    <Form.Item
                        label={
                            <Text type="span" weight="semiBold">
                                {tl('name')}
                            </Text>
                        }
                    >
                        <Form.Item
                            name="name"
                            rules={[
                                {
                                    required: true,
                                    message: t('businessAttribute.nameRequiredError'),
                                },
                                { whitespace: true },
                                { min: 1, max: 100 },
                            ]}
                            hasFeedback
                        >
                            <Input
                                placeholder={t('businessAttribute.namePlaceholder')}
                                data-testid="create-business-attribute-name"
                            />
                        </Form.Item>
                    </Form.Item>
                    <DataTypeSelectContainer>
                        <Form.Item
                            label={
                                <Text type="span" weight="semiBold">
                                    {t('businessAttribute.dataType')}
                                </Text>
                            }
                        >
                            <Form.Item
                                rules={[
                                    {
                                        required: true,
                                        message: t('businessAttribute.dataTypeRequiredError'),
                                    },
                                ]}
                                name="dataType"
                                data-testid="select-data-type"
                                noStyle
                            >
                                <DataTypeSelect placeholder={t('businessAttribute.dataTypePlaceholder')}>
                                    {DATA_TYPES.map((dataType: SchemaFieldDataType) => (
                                        <Select.Option key={dataType} value={dataType}>
                                            {dataType}
                                        </Select.Option>
                                    ))}
                                </DataTypeSelect>
                            </Form.Item>
                        </Form.Item>
                    </DataTypeSelectContainer>
                    <StyledItem
                        label={
                            <Text type="span" weight="semiBold">
                                <Trans
                                    t={t}
                                    i18nKey="businessAttribute.documentationOptionalLabel"
                                    components={{ optional: <OptionalWrapper /> }}
                                />
                            </Text>
                        }
                    >
                        <StyledButton variant="link" onClick={() => setIsDocumentationModalVisible(true)}>
                            <PencilSimple />
                            {documentation
                                ? t('businessAttribute.editDocumentation')
                                : t('businessAttribute.addDocumentation')}
                        </StyledButton>
                        {isDocumentationModalVisible && (
                            <DescriptionModal
                                title={t('businessAttribute.addDocumentation')}
                                onClose={() => setIsDocumentationModalVisible(false)}
                                onSubmit={addDocumentation}
                                description={documentation}
                            />
                        )}
                    </StyledItem>
                    <CollapsiblePanel
                        header={
                            <Text type="span" color="textSecondary">
                                {t('businessAttribute.advancedSection')}
                            </Text>
                        }
                    >
                        <Form.Item
                            label={
                                <Text type="span" weight="semiBold">
                                    {t('businessAttribute.entityIdLabel', {
                                        entityName: entityRegistry.getEntityName(EntityType.BusinessAttribute),
                                    })}
                                </Text>
                            }
                        >
                            <HelpText>{t('businessAttribute.customIdHelp')}</HelpText>
                            <Form.Item
                                name="id"
                                rules={[
                                    () => ({
                                        validator(_, value) {
                                            if (value && validateCustomUrnId(value)) {
                                                return Promise.resolve();
                                            }
                                            return Promise.reject(new Error(t('businessAttribute.invalidIdError')));
                                        },
                                    }),
                                ]}
                            >
                                <Input
                                    placeholder={t('businessAttribute.customIdPlaceholder')}
                                    onChange={(event) => setStagedId(event.target.value)}
                                />
                            </Form.Item>
                        </Form.Item>
                    </CollapsiblePanel>
                </Form>
            </Modal>
        </>
    );
}
