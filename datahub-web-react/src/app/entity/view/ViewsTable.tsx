import React from 'react';
import { Empty } from 'antd';
import { StyledTable } from '../shared/components/styled/StyledTable';
import { ActionsColumn, DescriptionColumn, NameColumn, ViewTypeColumn } from './select/ViewsTableColumns';
import { DataHubView } from '../../../types.generated';

type ViewsTableProps = {
    views: DataHubView[];
    onEditView: (urn) => void;
};

/**
 * This component renders a table of Views.
 */
export const ViewsTable = ({ views, onEditView }: ViewsTableProps) => {
    const tableColumns = [
        {
            title: '视图名称',
            dataIndex: 'name',
            key: 'name',
            render: (name, record) => <NameColumn name={name} record={record} onEditView={onEditView} />,
        },
        {
            title: '视图说明',
            dataIndex: 'description',
            key: 'description',
            render: (description) => <DescriptionColumn description={description} />,
        },
        {
            title: '视图类型',
            dataIndex: 'viewType',
            key: 'viewType',
            render: (viewType) => <ViewTypeColumn viewType={viewType} />,
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (record) => <ActionsColumn record={record} />,
        },
    ];

    /**
     * The data for the Views List.
     */
    const tableData = views.map((view) => ({
        ...view,
    }));

    return (
        <StyledTable
            columns={tableColumns}
            dataSource={tableData}
            rowKey="urn"
            locale={{
                emptyText: <Empty description="未找到视图!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
            }}
            pagination={false}
        />
    );
};
