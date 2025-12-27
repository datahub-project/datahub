import { Button, Pagination, Table, Typography } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';

import { CreateOrganizationModal } from '@app/organization/CreateOrganizationModal';

import { useListOrganizationsQuery } from '@graphql/organization.generated';

export const OrganizationsList = () => {
    const [page, setPage] = useState(1);
    const pageSize = 10;
    const [isCreateModalVisible, setIsCreateModalVisible] = useState(false);

    const { data, loading, refetch } = useListOrganizationsQuery({
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
            },
        },
    });

    const columns = [
        {
            title: 'Name',
            dataIndex: ['properties', 'name'],
            key: 'name',
            render: (text: string, record: any) => <Link to={`/organization/${record.urn}`}>{text || record.urn}</Link>,
        },
        {
            title: 'Description',
            dataIndex: ['properties', 'description'],
            key: 'description',
        },
    ];

    return (
        <div style={{ padding: 24 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
                <Typography.Title level={2}>Organizations</Typography.Title>
                <Button type="primary" onClick={() => setIsCreateModalVisible(true)}>
                    Create Organization
                </Button>
            </div>

            <Table
                columns={columns}
                dataSource={data?.listOrganizations?.organizations || []}
                loading={loading}
                pagination={false}
                rowKey="urn"
            />

            <div style={{ marginTop: 16, textAlign: 'right' }}>
                <Pagination
                    current={page}
                    pageSize={pageSize}
                    total={data?.listOrganizations?.total || 0}
                    onChange={setPage}
                />
            </div>

            <CreateOrganizationModal
                visible={isCreateModalVisible}
                onClose={() => setIsCreateModalVisible(false)}
                onCreate={() => {
                    setIsCreateModalVisible(false);
                    refetch();
                }}
            />
        </div>
    );
};
