import React from 'react';
import styled from 'styled-components';
import { Button, Table } from 'antd';
import { SpinProps } from 'antd/es/spin';
import { LoadingOutlined } from '@ant-design/icons';
import { useEntityData } from '../../../EntityContext';
import { useGetExternalRolesQuery } from '../../../../../../graphql/dataset.generated';
import { handleAccessRoles } from './utils';
import AccessManagerDescription from './AccessManagerDescription';

const StyledTable = styled(Table)`
    overflow: inherit;
    height: inherit;

    &&& .ant-table-cell {
        background-color: #fff;
    }
    &&& .ant-table-thead .ant-table-cell {
        font-weight: 600;
        font-size: 12px;
        color: '#898989';
    }
    &&
        .ant-table-thead
        > tr
        > th:not(:last-child):not(.ant-table-selection-column):not(.ant-table-row-expand-icon-cell):not(
            [colspan]
        )::before {
        border: 1px solid #f0f0f0;
    }
` as typeof Table;

const StyledSection = styled.section`
    background-color: #fff;
    color: black;
    width: 83px;
    text-align: center;
    border-radius: 3px;
    border: none;
    font-weight: bold;
`;

const AccessButton = styled(Button)`
    background-color: #1890ff;
    color: white;
    width: 80px;
    height: 30px;
    border-radius: 3.5px;
    border: none;
    font-weight: bold;
    &:hover {
        background-color: #18baff;
        color: white;
        width: 80px;
        height: 30px;
        border-radius: 3.5px;
        border: none;
        font-weight: bold;
    }
`;

export default function AccessManagement() {
    const { entityData } = useEntityData();
    const entityUrn = (entityData as any)?.urn;

    const { data: externalRoles, loading: isLoading } = useGetExternalRolesQuery({
        variables: { urn: entityUrn as string },
        skip: !entityUrn,
    });

    const columns = [
        {
            title: 'Role Name',
            dataIndex: 'name',
            key: 'name',
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            render: (roleDescription) => {
                return <AccessManagerDescription description={roleDescription} />;
            },
        },
        {
            title: 'Access Type',
            dataIndex: 'accessType',
            key: 'accessType',
        },
        {
            title: 'Access',
            dataIndex: 'hasAccess',
            key: 'hasAccess',
            render: (hasAccess, record) => {
                if (hasAccess) {
                    return <StyledSection>Provisioned</StyledSection>;
                }
                if (record?.url) {
                    return (
                        <AccessButton
                            onClick={(e) => {
                                e.preventDefault();
                                window.open(record.url);
                            }}
                        >
                            Request
                        </AccessButton>
                    );
                }
                return <StyledSection />;
            },
            hidden: true,
        },
    ];
    const spinProps: SpinProps = { indicator: <LoadingOutlined style={{ fontSize: 28 }} spin /> };
    return (
        <StyledTable
            loading={isLoading ? spinProps : false}
            dataSource={handleAccessRoles(externalRoles)}
            columns={columns}
            pagination={false}
        />
    );
}
