import React from 'react';
import { Empty } from 'antd';
import { StyledTable } from '../shared/components/styled/StyledTable';
import { ActionsColumn, DescriptionColumn, NameColumn, ViewTypeColumn } from './ViewsTableColumns';
import { DataHubView } from '../../../types.generated';

type ViewsTableProps = {
    views: DataHubView[];
    onEditView: (urn) => void;
    onDeleteView: (urn) => void;
};

/**
 * This component renders a table of View table.
 */
export const ViewsTable = ({ views, onEditView, onDeleteView }: ViewsTableProps) => {
    const tableColumns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            render: (name, record) => <NameColumn name={name} record={record} onEditView={onEditView} />,
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            render: (description) => <DescriptionColumn description={description} />,
        },
        {
            title: 'Visibility',
            dataIndex: 'viewType',
            key: 'viewType',
            render: (viewType) => <ViewTypeColumn viewType={viewType} />,
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            render: (record) => <ActionsColumn record={record} onEditView={onEditView} onDeleteView={onDeleteView} />,
        },
    ];

    /**
     * The data for the Views List.
     */
    const tableData = views.map((view) => ({
        urn: view.urn,
        name: view.name,
        description: view.description,
        viewType: view.viewType,
    }));

    return (
        <StyledTable
            columns={tableColumns}
            dataSource={tableData}
            rowKey="urn"
            locale={{
                emptyText: <Empty description="No Views found!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
            }}
            pagination={false}
        />
    );
};
