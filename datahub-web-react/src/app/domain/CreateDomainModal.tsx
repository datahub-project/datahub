import React, { useState } from 'react';
import styled from 'styled-components';
import { message, Button, Input, Modal, Typography, Form, Collapse, Tag } from 'antd';
import { useCreateDomainMutation } from '../../graphql/domain.generated';
import { useEnterKeyListener } from '../shared/useEnterKeyListener';
import { groupIdTextValidation } from '../shared/textUtil';
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
    onCreate: (urn: string, id: string | undefined, name: string, description: string) => void;
};

const SUGGESTED_DOMAIN_NAMES = ['Engineering', 'Marketing', 'Sales', 'Product'];

const ID_FIELD_NAME = 'id';
const NAME_FIELD_NAME = 'name';
const DESCRIPTION_FIELD_NAME = 'description';

export default function CreateDomainModal({ onClose, onCreate }: Props) {
    const [createDomainMutation] = useCreateDomainMutation();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [form] = Form.useForm();

    const onCreateDomain = () => {
        createDomainMutation({
            variables: {
                input: {
                    id: form.getFieldValue(ID_FIELD_NAME),
                    name: form.getFieldValue(NAME_FIELD_NAME),
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
                        content: `Created domain!`,
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
                message.error({ content: `Failed to create Domain!: \n ${e.message || ''}`, duration: 3 });
            });
        onClose();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createDomainButton',
    });

    return (
        <Modal
            title="Create new Domain"
            visible
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button id="createDomainButton" onClick={onCreateDomain} disabled={!createButtonEnabled}>
                        Create
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
                <Form.Item label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>Give your new Domain a name. </Typography.Paragraph>
                    <Form.Item
                        name={NAME_FIELD_NAME}
                        rules={[
                            {
                                required: true,
                                message: 'Enter a Domain name.',
                            },
                            { whitespace: true },
                            { min: 1, max: 50 },
                        ]}
                        hasFeedback
                    >
                        <Input placeholder="A name for your domain" />
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
                <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>
                        An optional description for your new domain. You can change this later.
                    </Typography.Paragraph>
                    <Form.Item
                        name={DESCRIPTION_FIELD_NAME}
                        rules={[{ whitespace: true }, { min: 1, max: 500 }]}
                        hasFeedback
                    >
                        <Input.TextArea placeholder="A description for your domain" />
                    </Form.Item>
                </Form.Item>
                <Collapse ghost>
                    <Collapse.Panel header={<Typography.Text type="secondary">Advanced</Typography.Text>} key="1">
                        <Form.Item label={<Typography.Text strong>Domain Id</Typography.Text>}>
                            <Typography.Paragraph>
                                By default, a random UUID will be generated to uniquely identify this domain. If
                                you&apos;d like to provide a custom id instead to more easily keep track of this domain,
                                you may provide it here. Be careful, you cannot easily change the domain id after
                                creation.
                            </Typography.Paragraph>
                            <Form.Item
                                name={ID_FIELD_NAME}
                                rules={[
                                    () => ({
                                        validator(_, value) {
                                            if (value && groupIdTextValidation(value)) {
                                                return Promise.resolve();
                                            }
                                            return Promise.reject(new Error('Please enter a valid Domain id'));
                                        },
                                    }),
                                ]}
                            >
                                <Input placeholder="engineering" />
                            </Form.Item>
                        </Form.Item>
                    </Collapse.Panel>
                </Collapse>
            </Form>
        </Modal>
    );
}
