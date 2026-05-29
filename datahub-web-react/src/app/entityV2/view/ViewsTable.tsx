import { Table, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

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
    const { t } = useTranslation('entity.views');

    const tableColumns = [
        {
            title: t('table.name'),
            dataIndex: 'name',
            key: 'name',
            width: '25%',
            render: (record) => <NameColumn name={record.name} record={record} onEditView={onEditView} />,
        },
        {
            title: t('table.description'),
            dataIndex: 'description',
            key: 'description',
            render: (record) => <DescriptionColumn description={record.description} />,
        },
        {
            title: t('table.type'),
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
                <Text size="md" weight="bold">
                    {t('emptyNoResults')}
                </Text>
            </EmptyContainer>
        );
    }

    return <Table columns={tableColumns} data={tableData} isScrollable />;
};
