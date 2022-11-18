import React, { useState } from 'react';
import styled from 'styled-components';
import { useApolloClient } from '@apollo/client';
import { message, Button, Input, Modal, Typography, Form, Collapse, Tag } from 'antd';
import { ListDomainsDocument, ListDomainsQuery, useCreateDomainMutation } from '../../graphql/domain.generated';
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
const DEFAULT_PAGE_SIZE = 25;

const addToListDomainsCache = (client, newDomain) => {
    // Read the data from our cache for this query.
    const currData: ListDomainsQuery | null = client.readQuery({
        query: ListDomainsDocument,
        variables: {
            input: {
                start: 0,
                count: DEFAULT_PAGE_SIZE,
                query: undefined,
            },
        },
    });
    // Add our new domain into the existing list.
    const newDomains = [newDomain, ...(currData?.listDomains?.domains || [])];
    // Write our data back to the cache.
    client.writeQuery({
        query: ListDomainsDocument,
        variables: {
            input: {
                start: 0,
                count: DEFAULT_PAGE_SIZE,
                query: undefined,
            },
        },
        data: {
            listDomains: {
                start: 0,
                count: (currData?.listDomains?.count || 0) + 1,
                total: (currData?.listDomains?.total || 0) + 1,
                domains: newDomains,
            },
        },
    });
};

export default function CreateDomainModal({ onClose, onCreate }: Props) {
    const [stagedName, setStagedName] = useState('');
    const [stagedDescription, setStagedDescription] = useState('');
    const [stagedId, setStagedId] = useState<string | undefined>(undefined);
    const [createDomainMutation] = useCreateDomainMutation();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(true);
    const [form] = Form.useForm();
    const client = useApolloClient();

    const onCreateDomain = () => {
        createDomainMutation({
            variables: {
                input: {
                    id: stagedId,
                    name: stagedName,
                    description: stagedDescription,
                },
            },
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.CreateDomainEvent,
                    });
                }
                message.success({
                    content: `Created domain!`,
                    duration: 3,
                });
                onCreate(data?.createDomain || '', stagedId, stagedName, stagedDescription);
                setStagedName('');
                setStagedDescription('');
                setStagedId(undefined);
                addToListDomainsCache(client, {
                    urn: data?.createDomain || '',
                    id: stagedId || '',
                    properties: {
                        name: stagedName,
                        description: stagedDescription,
                    },
                    ownership: null,
                    entities: null,
                });
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
                    <Button id="createDomainButton" onClick={onCreateDomain} disabled={createButtonEnabled}>
                        Create
                    </Button>
                </>
            }
        >
            <Form
                form={form}
                initialValues={{}}
                layout="vertical"
                onFieldsChange={() =>
                    setCreateButtonEnabled(form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <Form.Item label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>Give your new Domain a name. </Typography.Paragraph>
                    <Form.Item
                        name="name"
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
                        <Input
                            placeholder="A name for your domain"
                            value={stagedName}
                            onChange={(event) => setStagedName(event.target.value)}
                        />
                    </Form.Item>
                    <SuggestedNamesGroup>
                        {SUGGESTED_DOMAIN_NAMES.map((name) => {
                            return <ClickableTag onClick={() => setStagedName(name)}>{name}</ClickableTag>;
                        })}
                    </SuggestedNamesGroup>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>
                        An optional description for your new domain. You can change this later.
                    </Typography.Paragraph>
                    <Form.Item name="description" rules={[{ whitespace: true }, { min: 1, max: 500 }]} hasFeedback>
                        <Input
                            placeholder="A description for your domain"
                            value={stagedDescription}
                            onChange={(event) => setStagedDescription(event.target.value)}
                        />
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
                                name="domainId"
                                rules={[
                                    () => ({
                                        validator(_, value) {
                                            if (value && groupIdTextValidation(value)) {
                                                return Promise.resolve();
                                            }
                                            return Promise.reject(new Error('Please enter correct Domain name'));
                                        },
                                    }),
                                ]}
                            >
                                <Input
                                    placeholder="engineering"
                                    value={stagedId || ''}
                                    onChange={(event) => setStagedId(event.target.value)}
                                />
                            </Form.Item>
                        </Form.Item>
                    </Collapse.Panel>
                </Collapse>
            </Form>
        </Modal>
    );
}
