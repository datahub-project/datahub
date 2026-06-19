import { EditOutlined } from '@ant-design/icons';
import { Button, Collapse, Form, Input, Modal, Select, Typography, message } from 'antd';
import DOMPurify from 'dompurify';
import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { SchemaFieldDataType } from '@app/businessAttribute/businessAttributeUtils';
import DescriptionModal from '@app/entity/shared/components/legacy/DescriptionModal';
import { validateCustomUrnId } from '@app/shared/textUtil';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
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
    padding: 0;
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
                message.loading({ content: tf('updating'), duration: 2 });
                setTimeout(() => {
                    analytics.event({
                        type: EventType.CreateBusinessAttributeEvent,
                        name,
                    });
                    message.success({
                        content: t('businessAttribute.createSuccess', {
                            entityName: entityRegistry.getEntityName(EntityType.BusinessAttribute),
                        }),
                        duration: 2,
                    });
                    if (onCreateBusinessAttribute) {
                        onCreateBusinessAttribute();
                    }
                }, 2000);
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: t('businessAttribute.createError', { error: e.message || '' }),
                    duration: 3,
                });
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
                footer={
                    <>
                        <Button
                            onClick={onModalClose}
                            type="text"
                            data-testid="cancel-create-business-attribute-button"
                        >
                            {tc('cancel')}
                        </Button>
                        <Button
                            id="createBusinessAttributeButton"
                            onClick={onCreateNewBusinessAttribute}
                            disabled={createButtonEnabled}
                            data-testid="create-business-attribute-button"
                        >
                            {tc('create')}
                        </Button>
                    </>
                }
            >
                <Form
                    form={form}
                    initialValues={{ dataType: DATA_TYPES[2] }}
                    layout="vertical"
                    onFieldsChange={() =>
                        setCreateButtonEnabled(form.getFieldsError().some((field) => field.errors.length > 0))
                    }
                >
                    <Form.Item label={<Typography.Text strong>{tl('name')}</Typography.Text>}>
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
                        <Form.Item label={<Typography.Text strong>{t('businessAttribute.dataType')}</Typography.Text>}>
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
                            <Typography.Text strong>
                                <Trans
                                    t={t}
                                    i18nKey="businessAttribute.documentationOptionalLabel"
                                    components={{ optional: <OptionalWrapper /> }}
                                />
                            </Typography.Text>
                        }
                    >
                        <StyledButton type="link" onClick={() => setIsDocumentationModalVisible(true)}>
                            <EditOutlined />
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
                    <Collapse ghost>
                        <Collapse.Panel
                            header={
                                <Typography.Text type="secondary">
                                    {t('businessAttribute.advancedSection')}
                                </Typography.Text>
                            }
                            key="1"
                        >
                            <Form.Item
                                label={
                                    <Typography.Text strong>
                                        {t('businessAttribute.entityIdLabel', {
                                            entityName: entityRegistry.getEntityName(EntityType.BusinessAttribute),
                                        })}
                                    </Typography.Text>
                                }
                            >
                                <Typography.Paragraph>{t('businessAttribute.customIdHelp')}</Typography.Paragraph>
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
                        </Collapse.Panel>
                    </Collapse>
                </Form>
            </Modal>
        </>
    );
}
