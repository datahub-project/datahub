import { Button, Modal, Table, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import ColumnStats from '@app/entity/shared/tabs/Dataset/Stats/snapshot/ColumnStats';
import TableStats from '@app/entity/shared/tabs/Dataset/Stats/snapshot/TableStats';
import { formatBytes, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

import { DatasetProfile } from '@types';

export const ChartTable = styled(Table)`
    margin-top: 16px;
`;

export type Props = {
    profiles: Array<DatasetProfile>;
};

const bytesFormatter = (bytes: number) => {
    const formattedBytes = formatBytes(bytes);
    const fullBytes = formatNumberWithoutAbbreviation(bytes);
    return `${formattedBytes.number} ${formattedBytes.unit} (${fullBytes} bytes)`;
};

export default function ProfilingRunsChart({ profiles }: Props) {
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
            rowCount: profile.rowCount?.toString() || 'unknown',
            columnCount: profile.columnCount?.toString() || 'unknown',
            sizeInBytes: profile.sizeInBytes ? bytesFormatter(profile.sizeInBytes) : 'unknown',
        };
    });

    const tableColumns = [
        {
            title: 'Date',
            key: 'Date',
            dataIndex: 'timestamp',
            render: (title, record, index) => {
                return (
                    <Button type="text" onClick={() => showProfileModal(index)}>
                        <Typography.Text underline>{title}</Typography.Text>
                    </Button>
                );
            },
        },
        {
            title: 'Row Count',
            key: 'Row Count',
            dataIndex: 'rowCount',
        },
        {
            title: 'Column Count',
            key: 'Column Count',
            dataIndex: 'columnCount',
        },
        {
            title: 'Size',
            key: 'Size',
            dataIndex: 'sizeInBytes',
        },
    ];

    const selectedProfile = (selectedProfileIndex >= 0 && profiles[selectedProfileIndex]) || undefined;
    const profileModalTitle =
        selectedProfile &&
        `Showing profile from ${new Date(selectedProfile?.timestampMillis).toLocaleDateString()} at ${new Date(
            selectedProfile?.timestampMillis,
        ).toLocaleTimeString()}`;

    return (
        <>
            {selectedProfile && (
                <Modal width="100%" footer={null} title={profileModalTitle} open={showModal} onCancel={onClose}>
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
