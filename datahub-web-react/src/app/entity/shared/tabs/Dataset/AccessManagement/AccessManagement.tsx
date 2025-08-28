import { LoadingOutlined } from '@ant-design/icons';
import { Button, Table, Tooltip } from 'antd';
import { SpinProps } from 'antd/es/spin';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import AccessManagerDescription from '@app/entity/shared/tabs/Dataset/AccessManagement/AccessManagerDescription';
import { handleAccessRoles } from '@app/entity/shared/tabs/Dataset/AccessManagement/utils';

import { useGetExternalRolesQuery } from '@graphql/dataset.generated';

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
    
    /* Disabled/greyed out state when user already has access */
    &:disabled {
        background-color: #f5f5f5;
        color: #bfbfbf;
        cursor: not-allowed;
        border: 1px solid #d9d9d9;
        
        &:hover {
            background-color: #f5f5f5;
            color: #bfbfbf;
            border: 1px solid #d9d9d9;
        }
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
                if (record?.url || hasAccess) {
                    const button = (
                        <AccessButton
                            disabled={hasAccess}
                            onClick={(e) => {
                                if (!hasAccess) {
                                    e.preventDefault();
                                    window.open(record.url);
                                }
                            }}
                        >
                            {hasAccess ? 'Granted' : 'Request'}
                        </AccessButton>
                    );

                    // Show tooltip when user already has access
                    if (hasAccess) {
                        return (
                            <Tooltip 
                                title="You already have access to this role"
                                placement="top"
                            >
                                {button}
                            </Tooltip>
                        );
                    }

                    return button;
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
