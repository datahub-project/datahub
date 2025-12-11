/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Table, Text } from '@components';
import React from 'react';

import { AlignmentOptions } from '@components/theme/config';

import { EmptyContainer } from '@app/entityV2/view/ViewsList';
import {
    ActionsColumn,
    DescriptionColumn,
    NameColumn,
    ViewTypeColumn,
} from '@app/entityV2/view/select/ViewsTableColumns';

import { DataHubView } from '@types';

type ViewsTableProps = {
    views: DataHubView[];
    onEditView?: (urn) => void;
};

/**
 * This component renders a table of Views.
 */
export const ViewsTable = ({ views, onEditView }: ViewsTableProps) => {
    const tableColumns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            width: '25%',
            render: (record) => <NameColumn name={record.name} record={record} onEditView={onEditView} />,
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            render: (record) => <DescriptionColumn description={record.description} />,
        },
        {
            title: 'Type',
            dataIndex: 'viewType',
            key: 'viewType',
            width: '10%',
            render: (record) => <ViewTypeColumn viewType={record.viewType} />,
        },
        {
            title: '',
            dataIndex: '',
            key: 'x',
            width: '5%',
            alignment: 'right' as AlignmentOptions,
            render: (record) => <ActionsColumn record={record} />,
        },
    ];

    /**
     * The data for the Views List.
     */
    const tableData =
        views.map((view) => ({
            ...view,
        })) || [];

    if (!views.length) {
        return (
            <EmptyContainer>
                <Text size="md" color="gray" weight="bold">
                    No results!
                </Text>
            </EmptyContainer>
        );
    }

    return <Table columns={tableColumns} data={tableData} isScrollable />;
};
