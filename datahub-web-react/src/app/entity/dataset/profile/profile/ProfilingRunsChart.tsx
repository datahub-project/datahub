import { Button, Col, Modal, Table, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { DatasetProfile } from '../../../../../types.generated';
import DataProfileView from './DataProfileView';

export type Props = {
    profiles: Array<DatasetProfile>;
};

export const ChartTable = styled(Table)`
    margin: 12px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
`;

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
        columns: ['Reported At', 'Row Count', 'Column Count'],
    };

    const columns = [
        {
            title: chartData.columns[0],
            key: chartData.columns[0],
            dataIndex: chartData.columns[0],
            render: (title, record, index) => {
                return (
                    <Button type="text" onClick={() => showProfileModal(index)}>
                        <Typography.Text underline>{title}</Typography.Text>
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
                <ChartTable
                    scroll={{ y: 400 }}
                    bordered
                    style={{ width: '100%', maxHeight: 440 }}
                    columns={columns}
                    dataSource={tableData}
                    pagination={false}
                    size="small"
                />
            </Col>
        </>
    );
}
