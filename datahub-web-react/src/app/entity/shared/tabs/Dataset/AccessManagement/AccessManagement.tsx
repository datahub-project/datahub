import { LoadingOutlined } from '@ant-design/icons';
import { Table } from 'antd';
import { SpinProps } from 'antd/es/spin';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { AccessButton, RoleAccessData } from '@app/entity/shared/tabs/Dataset/AccessManagement/AccessButtonHelpers';
import AccessManagerDescription from '@app/entity/shared/tabs/Dataset/AccessManagement/AccessManagerDescription';
import { handleAccessRoles } from '@app/entity/shared/tabs/Dataset/AccessManagement/utils';

import { useGetExternalRolesQuery } from '@graphql/dataset.generated';

const StyledTable = styled(Table)`
    overflow: inherit;
    height: inherit;

    &&& .ant-table-cell {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
    &&& .ant-table-thead .ant-table-cell {
        font-weight: 600;
        font-size: 12px;
        color: ${(props) => props.theme.colors.textSecondary};
    }
    &&
        .ant-table-thead
        > tr
        > th:not(:last-child):not(.ant-table-selection-column):not(.ant-table-row-expand-icon-cell):not(
            [colspan]
        )::before {
        border: 1px solid ${(props) => props.theme.colors.border};
    }
` as typeof Table;

/**
 * Styled component for empty access state display
 */
const EmptyAccessSection = styled.section`
    background-color: ${(props) => props.theme.colors.bgSurface};
    color: ${(props) => props.theme.colors.text};
    width: 83px;
    text-align: center;
    border-radius: 3px;
    border: none;
    font-weight: bold;
`;

/**
 * Renders the access button or empty state based on role data
 */
const renderAccessCell = (hasAccess: boolean, record: RoleAccessData) => {
    const roleData = { hasAccess, url: record.url, name: record.name };

    return <AccessButton roleData={roleData} fallback={<EmptyAccessSection />} />;
};

/**
 * AccessManagement component displays a table of roles with access request functionality.
 * Shows "Granted" (disabled) buttons for roles the user already has access to,
 * and "Request" (enabled) buttons for roles they can request access to.
 */
export default function AccessManagement() {
    const { t } = useTranslation('entity.profile.access');
    const { t: tc } = useTranslation('common.labels');
    const { entityData } = useEntityData();
    const entityUrn = entityData?.urn as string;

    const { data: externalRoles, loading: isLoading } = useGetExternalRolesQuery({
        variables: { urn: entityUrn },
        skip: !entityUrn,
    });

    const columns = [
        {
            title: t('accessManagement.columnRoleName'),
            dataIndex: 'name',
            key: 'name',
        },
        {
            title: tc('description'),
            dataIndex: 'description',
            key: 'description',
            render: (roleDescription) => {
                return <AccessManagerDescription description={roleDescription} />;
            },
        },
        {
            title: t('accessManagement.columnAccessType'),
            dataIndex: 'accessType',
            key: 'accessType',
        },
        {
            title: t('accessManagement.columnAccess'),
            dataIndex: 'hasAccess',
            key: 'hasAccess',
            render: renderAccessCell,
            hidden: true,
        },
    ];

    const spinProps: SpinProps = {
        indicator: <LoadingOutlined style={{ fontSize: 28 }} spin />,
    };

    const tableData = handleAccessRoles(externalRoles);

    return (
        <StyledTable
            loading={isLoading ? spinProps : false}
            dataSource={tableData}
            columns={columns}
            pagination={false}
            aria-label={t('accessManagement.tableAriaLabel')}
        />
    );
}
