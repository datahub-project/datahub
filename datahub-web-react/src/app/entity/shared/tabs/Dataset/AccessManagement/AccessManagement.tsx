import React from 'react';
import styled from 'styled-components';
import { Table } from 'antd';
import { useBaseEntity } from '../../../EntityContext';
import { GetDatasetQuery, useGetExternalRolesQuery } from '../../../../../../graphql/dataset.generated';
import { useGetMeQuery } from '../../../../../../graphql/me.generated';
import handleExternalRoles from './AccessRolesDetail';
import AccessManagerDescription from './AccessManagerDescription';

export default function AccessManagement() {
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
            > th:not(:last-child):not(.ant-table-selection-column):not(.ant-table-row-expand-icon-cell):not([colspan])::before {
            border: 1px solid #f0f0f0;
        }
    ` as typeof Table;

    const { data: loggedInUser } = useGetMeQuery({ fetchPolicy: 'cache-first' });
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const { data: externalRoles } = useGetExternalRolesQuery({
        variables: { urn: baseEntity?.dataset?.urn as string },
        skip: !baseEntity?.dataset?.urn,
    });

    const columns = [
        {
            title: 'RoleName',
            dataIndex: 'name',
            key: 'name',
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            render: (text) => {
                return <AccessManagerDescription description={text} />;
            },
        },
        {
            title: 'AccessType',
            dataIndex: 'accesstype',
            key: 'accesstype',
        },
        {
            title: 'Access',
            dataIndex: 'access',
            key: 'access',
            render: (text, record) => {
                if (text) {
                    return (
                        <div
                            style={{
                                backgroundColor: '#fff',
                                color: 'black',
                                width: '80px',
                                height: '30px',
                                borderRadius: '3px',
                                border: 'none',
                                fontWeight: 'bold',
                            }}
                        >
                            Provisioned
                        </div>
                    );
                }
                return (
                    <button
                        type="button"
                        style={{
                            backgroundColor: '#1890ff',
                            color: 'white',
                            width: '80px',
                            height: '30px',
                            borderRadius: '3.5px',
                            border: 'none',
                            fontWeight: 'bold',
                        }}
                        onClick={(e) => {
                            e.preventDefault();
                            window.open(record.url);
                        }}
                    >
                        Request
                    </button>
                );
            },
            hidden: true,
        },
    ];

    return (
        <div>
            <StyledTable
                dataSource={handleExternalRoles(externalRoles, loggedInUser)}
                columns={columns}
                pagination={false}
            />
        </div>
    );
}
