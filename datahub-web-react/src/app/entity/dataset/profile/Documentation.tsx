import { Button, Form, Input, Space, Table, Typography } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import {
    CorpUser,
    EntityType,
    InstitutionalMemoryMetadata,
    InstitutionalMemoryUpdate,
} from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useEntityData } from '../../shared/EntityContext';
import analytics, { EventType, EntityActionType } from '../../../analytics';

export type Props = {
    authenticatedUserUrn?: string;
    authenticatedUserUsername?: string;
    documents: Array<InstitutionalMemoryMetadata>;
    updateDocumentation: (update: InstitutionalMemoryUpdate) => void;
};

function formatDateString(time: number) {
    const date = new Date(time);
    return date.toLocaleDateString('en-US');
}

function FormInput({ name, placeholder, type }: { name: string; placeholder: string; type: 'url' | 'string' }) {
    return (
        <Form.Item
            name={name}
            style={{
                margin: 0,
            }}
            rules={[
                {
                    required: true,
                    type,
                    message: `Please provide a ${name}!`,
                },
            ]}
        >
            <Input placeholder={placeholder} />
        </Form.Item>
    );
}

export default function Documentation({
    authenticatedUserUrn,
    documents,
    updateDocumentation,
    authenticatedUserUsername,
}: Props) {
    const entityRegistry = useEntityRegistry();

    const [form] = Form.useForm();
    const [editingIndex, setEditingIndex] = useState(-1);
    const [stagedDocs, setStagedDocs] = useState(documents);

    useEffect(() => {
        setStagedDocs(documents);
    }, [documents]);

    const tableData = useMemo(
        () =>
            stagedDocs.map((doc, index) => ({
                key: index,
                author: { urn: doc.author.urn, username: doc.author.username, type: EntityType.CorpUser },
                url: doc.url,
                description: doc.description,
                createdAt: doc.created.time,
            })),
        [stagedDocs],
    );

    const isEditing = (record: any) => record.key === editingIndex;

    const onAdd = (authorUrn: string, authorUsername: string) => {
        setEditingIndex(stagedDocs.length);

        form.setFieldsValue({
            url: '',
            description: '',
        });

        const newDoc = {
            url: '',
            description: '',
            label: '',
            author: { urn: authorUrn, username: authorUsername, type: EntityType.CorpUser },
            created: {
                time: Date.now(),
            },
        };

        const newStagedDocs = [...stagedDocs, newDoc];
        setStagedDocs(newStagedDocs);
    };

    const { urn, entityType } = useEntityData();

    const onSave = async (record: any) => {
        const row = await form.validateFields();

        const updatedInstitutionalMemory = stagedDocs.map((doc, index) => {
            if (record.key === index) {
                return {
                    url: row.url,
                    description: row.description,
                    author: doc.author.urn,
                    createdAt: doc.created.time,
                };
            }
            return {
                author: doc.author.urn,
                url: doc.url,
                description: doc.description,
                createdAt: doc.created.time,
            };
        });
        updateDocumentation({ elements: updatedInstitutionalMemory });
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateDescription,
            entityType,
            entityUrn: urn,
        });
        setEditingIndex(-1);
    };

    const onCancel = () => {
        const newStagedDocs = stagedDocs.filter((_, index) => index !== editingIndex);
        setStagedDocs(newStagedDocs);
        setEditingIndex(-1);
    };

    const onDelete = (index: number) => {
        const newDocs = stagedDocs.filter((_, i) => !(index === i));
        const updatedInstitutionalMemory = newDocs.map((doc) => ({
            author: doc.author.urn,
            url: doc.url,
            description: doc.description,
        }));
        updateDocumentation({ elements: updatedInstitutionalMemory });
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateDescription,
            entityType,
            entityUrn: urn,
        });
        setStagedDocs(newDocs);
    };

    const tableColumns = [
        {
            title: 'Document',
            dataIndex: 'description',
            render: (description: string, record: any) => {
                return isEditing(record) ? (
                    <Space>
                        <FormInput name="description" placeholder="Enter a description" type="string" />
                        <FormInput name="url" placeholder="Enter a URL" type="url" />
                    </Space>
                ) : (
                    <Typography.Link href={record.url} target="_blank">
                        {description}
                    </Typography.Link>
                );
            },
        },
        {
            title: 'Author',
            dataIndex: 'author',
            render: (user: CorpUser) => (
                <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${user.urn}`}>{user.username}</Link>
            ),
        },
        {
            title: 'Date Added',
            dataIndex: 'createdAt',
            render: (createdAt: number) => formatDateString(createdAt),
        },
        {
            title: '',
            key: 'action',
            render: (_: string, record: any) => {
                return isEditing(record) ? (
                    <Space>
                        <Button type="link" onClick={() => onSave(record)}>
                            Save
                        </Button>
                        <Button type="link" style={{ color: 'grey' }} onClick={onCancel}>
                            Cancel
                        </Button>
                    </Space>
                ) : (
                    <Button type="link" style={{ color: 'red' }} onClick={() => onDelete(record.key)}>
                        Remove
                    </Button>
                );
            },
        },
    ];

    return (
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
            <Typography.Title level={3}>Documentation</Typography.Title>
            <Typography.Paragraph>Keep track of documentation about this Dataset.</Typography.Paragraph>
            <Form form={form} component={false}>
                <Table bordered pagination={false} columns={tableColumns} dataSource={tableData} />
            </Form>
            {authenticatedUserUrn && authenticatedUserUsername && editingIndex < 0 && (
                <Button type="link" onClick={() => onAdd(authenticatedUserUrn, authenticatedUserUsername)}>
                    <b> + </b> Add a link
                </Button>
            )}
        </Space>
    );
}
