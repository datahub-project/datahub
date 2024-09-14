import { Button, Col, Modal, Table, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { DatasetProfile } from '../../../../../../../types.generated';
import DataProfileView from '../../snapshot/SnapshotStatsView';

export const ChartTable = styled(Table)`
    margin: 12px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
`;

export type Props = {
    profiles: Array<DatasetProfile>;
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
        };
    });

    const tableColumns = [
        {
            title: 'Recent Profiles',
            key: 'Recent Profiles',
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
                    <DataProfileView profile={selectedProfile} />
                </Modal>
            )}
            <Col sm={24} md={24} lg={12} xl={12}>
                <ChartTable
                    scroll={{ y: 400 }}
                    bordered
                    style={{ width: '100%', maxHeight: 440 }}
                    columns={tableColumns}
                    dataSource={tableData}
                    pagination={false}
                    size="small"
                />
            </Col>
        </>
    );
}
