import React, { useState } from 'react';
import styled from 'styled-components';
import { Trans } from 'react-i18next';
import {t} from "i18next";
import { message, Button, Input, Modal, Typography, Form, Collapse, Tag } from 'antd';
import { useCreateDomainMutation } from '../../graphql/domain.generated';
import { useEnterKeyListener } from '../shared/useEnterKeyListener';
import { validateCustomUrnId } from '../shared/textUtil';
import analytics, { EventType } from '../analytics';


const SuggestedNamesGroup = styled.div`
    margin-top: 12px;
`;

const ClickableTag = styled(Tag)`
    :hover {
        cursor: pointer;
    }
`;

type Props = {
    onClose: () => void;
    onCreate: (urn: string, id: string | undefined, name: string, description: string | undefined) => void;
};

const SUGGESTED_DOMAIN_NAMES = ['Engineering', 'Marketing', 'Sales', 'Product'];

const ID_FIELD_NAME = 'id';
const NAME_FIELD_NAME = 'name';
const DESCRIPTION_FIELD_NAME = 'description';
const DOMAIN_ID_PARAGRAPH = "By default, a random UUID will be generated to uniquely identify this domain. If you would like to provide a custom id instead to more easily keep track of this domain, you may provide it here. Be careful, you cannot easily change the domain id after creation.";

export default function CreateDomainModal({ onClose, onCreate }: Props) {
    const [createDomainMutation] = useCreateDomainMutation();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [form] = Form.useForm();

    const onCreateDomain = () => {
        createDomainMutation({
            variables: {
                input: {
                    id: form.getFieldValue(ID_FIELD_NAME),
                    name:form.getFieldValue(NAME_FIELD_NAME),
                    description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
                },
            },
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.CreateDomainEvent,
                    });
                    message.success({
                        content: t('Created_domain'),
                        duration: 3,
                    });
                    onCreate(
                        data?.createDomain || '',
                        form.getFieldValue(ID_FIELD_NAME),
                        form.getFieldValue(NAME_FIELD_NAME),
                        form.getFieldValue(DESCRIPTION_FIELD_NAME),
                    );
                    form.resetFields();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: t('Failed to create Domain')`!: \n ${e.message || ''}`, duration: 3 });
            });
        onClose();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createDomainButton',
    });



    return (
        <Modal
            title={t('Create new Domain')}
            visible
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        <Trans>Cancel</Trans>
                    </Button>
                    <Button
                        id="createDomainButton"
                        data-testid="create-domain-button"
                        onClick={onCreateDomain}
                        disabled={!createButtonEnabled}
                    >
                        <Trans>Create</Trans>
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
                <Form.Item label={<Typography.Text strong><Trans>Name</Trans></Typography.Text>}>
                    <Typography.Paragraph><Trans>Give your new Domain a name</Trans>. </Typography.Paragraph>
                    <Form.Item
                        name={NAME_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: t('Enter a Domain name.'),
                            },
                            { whitespace: true },
                            { min: 1, max: 150 },
                        ]}
                        hasFeedback
                    >
                        <Input data-testid="create-domain-name" placeholder="A name for your domain" />
                    </Form.Item>
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
                </Form.Item>
                <Form.Item label={<Typography.Text strong><Trans>Description</Trans></Typography.Text>}>
                    <Typography.Paragraph>
                        <Trans>An optional description for your new domain</Trans>. <Trans>You can change this later</Trans>.
                    </Typography.Paragraph>
                    <Form.Item
                        name={DESCRIPTION_FIELD_NAME}
                        rules={[{ whitespace: true }, { min: 1, max: 500 }]}
                        hasFeedback
                    >
                        <Input.TextArea placeholder= "A description for your domain" />
                    </Form.Item>
                </Form.Item>
                <Collapse ghost>
                    <Collapse.Panel header={<Typography.Text type="secondary"><Trans>Advanced</Trans></Typography.Text>} key="1">
                        <Form.Item label={<Typography.Text strong><Trans>Domain</Trans> Id</Typography.Text>}>
                            <Typography.Paragraph>
                                <Trans>{DOMAIN_ID_PARAGRAPH}</Trans>
                            </Typography.Paragraph>
                            <Form.Item
                                name={ID_FIELD_NAME}
                                rules={[
                                    () => ({
                                        validator(_, value) {
                                            if (value && validateCustomUrnId(value)) {
                                                return Promise.resolve();
                                            }
                                            return Promise.reject(new Error(t('Please enter a valid Domain id')));
                                        },
                                    }),
                                ]}
                            >
                                <Input data-testid="create-domain-id" placeholder="engineering" />
                            </Form.Item>
                        </Form.Item>
                    </Collapse.Panel>
                </Collapse>
            </Form>
        </Modal>
    );
}