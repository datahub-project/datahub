import { Pill, Table } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Column } from '@components/components/Table/types';

import { EnvironmentActions } from '@app/settingsV2/environments/EnvironmentActions';
import { isDeprecated } from '@app/settingsV2/environments/environments.utils';

import { CustomFabricType } from '@types';

const NameCell = styled.div`
    display: flex;
    flex-direction: column;
`;

const NameText = styled.span`
    font-weight: 700;
`;

const IdText = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
`;

type Props = {
    environments: CustomFabricType[];
    canManage: boolean;
    onEdit: (environment: CustomFabricType) => void;
    refetch: () => void;
};

export const EnvironmentTable = ({ environments, canManage, onEdit, refetch }: Props) => {
    const { t } = useTranslation('settings.environments');

    const columns: Column<CustomFabricType>[] = [
        {
            title: t('table.environment'),
            key: 'environment',
            width: '30%',
            render: (record) => (
                <NameCell>
                    <NameText>{record.properties?.displayName || record.id}</NameText>
                    {record.properties?.displayName && <IdText>{record.id}</IdText>}
                </NameCell>
            ),
        },
        {
            title: t('table.description'),
            key: 'description',
            width: '45%',
            render: (record) => record.properties?.description || '',
        },
        {
            title: t('table.status'),
            key: 'status',
            width: '15%',
            render: (record) =>
                isDeprecated(record) ? <Pill label={t('status.deprecated')} color="yellow" size="sm" /> : null,
        },
        {
            title: '',
            key: 'actions',
            width: '10%',
            alignment: 'right',
            render: (record) =>
                canManage ? (
                    <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                        <EnvironmentActions environment={record} onEdit={onEdit} refetch={refetch} />
                    </div>
                ) : null,
        },
    ];

    return (
        <Table<CustomFabricType>
            columns={columns}
            data={environments}
            rowKey={(record) => record.urn}
            isScrollable
            style={{ tableLayout: 'fixed' }}
        />
    );
};
