import { Button, Col, Modal, Table, Typography } from 'antd';
import React, { useState } from 'react';
import { DatasetProfile } from '../../../../../types.generated';
import { ChartCard } from '../../../../analyticsDashboard/components/ChartCard';
import { ChartContainer } from '../../../../analyticsDashboard/components/ChartContainer';
import DataProfileView from './DataProfileView';

export type Props = {
    profiles: Array<DatasetProfile>;
};

export default function ProfilingRunsChart({ profiles }: Props) {
    const [showModal, setShowModal] = useState(false);
    const [selectedProfileIndex, setSelectedProfileIndex] = useState(-1);

    const showProfileModal = (index: number) => {
        // Show a specific profile from the list.
        setSelectedProfileIndex(index);
        setShowModal(true);
    };

    const onClose = () => {
        setShowModal(false);
        setSelectedProfileIndex(-1);
    };

    const rows = profiles.map((profile) => {
        const profileDate = new Date(profile.timestampMillis);
        return {
            values: [
                `${profileDate.toLocaleDateString()} at ${profileDate.toLocaleTimeString()}`,
                profile.rowCount?.toString() || 'unknown',
                profile.columnCount?.toString() || 'unknown',
            ],
        };
    });

    const chartData = {
        title: 'Recent Profiles',
        rows,
        columns: ['Run Time', 'Row Count', 'Column Count'],
    };

    const columns = [
        {
            title: chartData.columns[0],
            key: chartData.columns[0],
            dataIndex: chartData.columns[0],
            render: (title, record, index) => {
                return (
                    <Button type="text" onClick={() => showProfileModal(index)}>
                        <b>{title}</b>
                    </Button>
                );
            },
        },
        {
            title: chartData.columns[1],
            key: chartData.columns[1],
            dataIndex: chartData.columns[1],
        },
        {
            title: chartData.columns[2],
            key: chartData.columns[2],
            dataIndex: chartData.columns[2],
        },
    ];

    const tableData = chartData.rows.map((row) =>
        row.values.reduce((acc, value, i) => ({ ...acc, [chartData.columns[i]]: value }), {}),
    );

    const selectedProfile = (selectedProfileIndex >= 0 && profiles[selectedProfileIndex]) || undefined;
    const profileModalTitle =
        (selectedProfile &&
            `Showing profile from ${new Date(selectedProfile?.timestampMillis).toLocaleDateString()} at ${new Date(
                selectedProfile?.timestampMillis,
            ).toLocaleTimeString()}`) ||
        '';

    return (
        <>
            {selectedProfile && (
                <Modal width="100%" footer={null} title={profileModalTitle} visible={showModal} onCancel={onClose}>
                    <DataProfileView profile={selectedProfile} />
                </Modal>
            )}
            <Col sm={24} md={24} lg={12} xl={12}>
                <ChartCard shouldScroll>
                    <ChartContainer>
                        <div style={{ width: '100%', marginBottom: 20 }}>
                            <Typography.Title level={5}>{chartData.title}</Typography.Title>
                        </div>
                        <Table
                            style={{ width: '100%', padding: 12 }}
                            columns={columns}
                            dataSource={tableData}
                            pagination={false}
                            size="small"
                        />
                    </ChartContainer>
                </ChartCard>
            </Col>
        </>
    );
}
