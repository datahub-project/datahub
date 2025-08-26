import { Alert, Modal, Table, message } from 'antd';
import React, { useState } from 'react';

import { ActionsMenu } from '@app/connections/components/ActionMenu';
import { useDeleteConnection } from '@app/connections/hooks';
import { Message } from '@app/shared/Message';

type Props = {
    connections: {
        data: any;
        loading: boolean;
        error: any;
        refetch: () => void;
    };
    form: any;
};

export const ConnectionsTable = ({ connections, form }: Props) => {
    const FormComponent = form;

    // Props
    const { data, loading, error, refetch } = connections;

    // State
    const [isModalOpen, setIsModalOpen] = useState<string | undefined>();

    // Graph
    const { removeConnection } = useDeleteConnection();

    const dataSource = data?.map((connection: any) => ({
        key: connection?.urn,
        name: connection?.details?.name,
    }));

    const columns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
        },
        {
            title: 'Actions',
            key: 'actions',
            width: '10%',
            render: (_: any, record: any) => (
                <>
                    <ActionsMenu
                        items={[
                            {
                                key: 'edit',
                                onClick: () => setIsModalOpen(record.key),
                                disabled: false,
                                icon: 'Edit',
                                label: 'Edit',
                                tooltip: `Update the connection configuration.`,
                            },
                            {
                                key: 'delete',
                                onClick: () => {
                                    if (!record.key) return;
                                    removeConnection(record.key);
                                    refetch();
                                    setTimeout(() => {
                                        message.success({ content: 'Connection deleted successfully', duration: 3 });
                                    }, 3000);
                                },
                                disabled: false,
                                icon: 'Delete',
                                label: 'Delete',
                                tooltip: 'Permanently delete this connection.',
                            },
                        ]}
                    />
                    <Modal
                        open={record.key === isModalOpen}
                        onOk={() => setIsModalOpen(undefined)}
                        onCancel={() => setIsModalOpen(undefined)}
                        footer={null}
                    >
                        <FormComponent
                            urn={record.key}
                            connections={connections}
                            disclosure={{
                                closeModal: () => {
                                    setIsModalOpen(undefined);
                                    refetch();
                                },
                            }}
                        />
                    </Modal>
                </>
            ),
        },
    ];

    if (loading && !error) {
        return <Message type="loading" content="Fetching connections…" style={{ marginTop: '10%' }} />;
    }

    if (error) {
        return <Alert type="error" message={error || `Failed to fetch connections`} />;
    }

    return <Table dataSource={dataSource} columns={columns} size="small" style={{ width: '100%' }} />;
};
