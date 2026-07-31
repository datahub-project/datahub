import { Modal } from '@components';
import { Button, Table, Typography } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { FULL_TABLE_PARTITION_KEYS } from '@app/entityV2/shared/tabs/Dataset/Stats/constants';
import ColumnStats from '@app/entityV2/shared/tabs/Dataset/Stats/snapshot/ColumnStats';
import TableStats from '@app/entityV2/shared/tabs/Dataset/Stats/snapshot/TableStats';
import { formatBytes, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

import { DatasetProfile } from '@types';

const ChartTable = styled(Table)`
    margin-top: 16px;
`;

type Props = {
    profiles: Array<DatasetProfile>;
    areAllProfilesPartitioned: boolean;
};

const bytesFormatter = (bytes: number) => {
    const formattedBytes = formatBytes(bytes);
    const fullBytes = formatNumberWithoutAbbreviation(bytes);
    return `${formattedBytes.number} ${formattedBytes.unit} (${fullBytes} bytes)`;
};

export default function ProfilingRunsChart({ profiles, areAllProfilesPartitioned }: Props) {
    const { t } = useTranslation('entity.profile.stats');
    const [showModal, setShowModal] = useState(false);
    const [selectedProfileIndex, setSelectedProfileIndex] = useState(-1);

    const showProfileModal = (index: number) => {
        setSelectedProfileIndex(index);
        setShowModal(true);
    };

    const onClose = () => {
        setShowModal(false);
        setSelectedProfileIndex(-1);
    };

    const tableData = profiles.map((profile) => {
        const profileDate = new Date(profile.timestampMillis);
        return {
            timestamp: `${profileDate.toLocaleDateString()} at ${profileDate.toLocaleTimeString()}`,
            rowCount: profile.rowCount?.toString() || t('profilingRunsChart.unknown'),
            columnCount: profile.columnCount?.toString() || t('profilingRunsChart.unknown'),
            sizeInBytes: profile.sizeInBytes ? bytesFormatter(profile.sizeInBytes) : t('profilingRunsChart.unknown'),
            partition: profile.partitionSpec?.partition || '',
        };
    });

    const tableColumns = [
        {
            title: areAllProfilesPartitioned
                ? t('profilingRunsChart.partitionColumn')
                : t('profilingRunsChart.dateColumn'),
            key: 'Date',
            dataIndex: 'timestamp',
            render: (title, record, index) => {
                return (
                    <Button type="text" onClick={() => showProfileModal(index)}>
                        <Typography.Text underline>
                            {FULL_TABLE_PARTITION_KEYS.includes(record.partition) ? title : record.partition}
                        </Typography.Text>
                    </Button>
                );
            },
        },
        {
            title: t('profilingRunsChart.rowCountColumn'),
            key: 'Row Count',
            dataIndex: 'rowCount',
        },
        {
            title: t('profilingRunsChart.columnCountColumn'),
            key: 'Column Count',
            dataIndex: 'columnCount',
        },
        {
            title: t('profilingRunsChart.sizeColumn'),
            key: 'Size',
            dataIndex: 'sizeInBytes',
        },
    ];

    const selectedProfile = (selectedProfileIndex >= 0 && profiles[selectedProfileIndex]) || undefined;
    const profileModalTitle =
        selectedProfile &&
        t('profilingRunsChart.showingProfile', {
            date: new Date(selectedProfile?.timestampMillis).toLocaleDateString(),
            time: new Date(selectedProfile?.timestampMillis).toLocaleTimeString(),
        });

    return (
        <>
            {selectedProfile && (
                <Modal
                    buttons={[]}
                    width="100%"
                    title={profileModalTitle || t('profilingRunsChart.profileFallbackTitle')}
                    open={showModal}
                    onCancel={onClose}
                >
                    <TableStats
                        rowCount={selectedProfile.rowCount || -1}
                        columnCount={selectedProfile.columnCount || -1}
                    />
                    <ColumnStats columnStats={selectedProfile.fieldProfiles || []} />
                </Modal>
            )}
            <ChartTable
                scroll={{ y: 400 }}
                bordered
                style={{ width: '100%', maxHeight: 440 }}
                columns={tableColumns}
                dataSource={tableData}
                pagination={false}
                size="small"
            />
        </>
    );
}
