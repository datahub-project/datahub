import { Button, Form, Input, Space, Table, Typography } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { EntityType, InstitutionalMemoryMetadata, InstitutionalMemoryUpdate } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';

export type Props = {
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

export default function Documentation({ documents, updateDocumentation }: Props) {
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
                author: doc.author,
                url: doc.url,
                description: doc.description,
                createdAt: doc.created.time,
            })),
        [stagedDocs],
    );

    const isEditing = (record: any) => record.key === editingIndex;

    const onAdd = () => {
        setEditingIndex(stagedDocs.length);

        form.setFieldsValue({
            url: '',
            description: '',
        });

        const newDoc = {
            url: '',
            description: '',
            author: localStorage.getItem('userUrn') as string,
            created: {
                time: Date.now(),
            },
        };

        const newStagedDocs = [...stagedDocs, newDoc];
        setStagedDocs(newStagedDocs);
    };

    const onSave = async (record: any) => {
        const row = await form.validateFields();

        const updatedInstitutionalMemory = stagedDocs.map((doc, index) => {
            if (record.key === index) {
                return {
                    url: row.url,
                    description: row.description,
                    author: doc.author,
                    createdAt: doc.created.time,
                };
            }
            return {
                author: doc.author,
                url: doc.url,
                description: doc.description,
                createdAt: doc.created.time,
            };
        });
        updateDocumentation({ elements: updatedInstitutionalMemory });
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
            author: doc.author,
            url: doc.url,
            description: doc.description,
        }));
        updateDocumentation({ elements: updatedInstitutionalMemory });
        setStagedDocs(newDocs);
    };

    const tableColumns = [
        {
            title: 'URL',
            dataIndex: 'url',
            render: (url: string, record: any) => {
                return isEditing(record) ? (
                    <FormInput name="url" placeholder="Enter a URL" type="url" />
                ) : (
                    <a href={url}>{url}</a>
                );
            },
        },
        {
            title: 'Description',
            dataIndex: 'description',
            render: (description: string, record: any) => {
                return isEditing(record) ? (
                    <FormInput name="description" placeholder="Enter a description" type="string" />
                ) : (
                    description
                );
            },
        },
        {
            title: 'Author',
            dataIndex: 'author',
            render: (authorUrn: string) => (
                <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${authorUrn}`}>{authorUrn}</Link>
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
                <Table pagination={false} columns={tableColumns} dataSource={tableData} />
            </Form>
            {editingIndex < 0 && (
                <Button type="link" onClick={onAdd}>
                    <b> + </b> Add a link
                </Button>
            )}
        </Space>
    );
}
