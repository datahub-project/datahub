import React, { useState } from 'react';
import './MetricsTab.less';
import { Table } from 'antd';
import { DatasetMetricsProps, getFormattedScore } from './Metrics';
//import { toTitleCaseChangeCase } from '../../../../../dataforge/shared';
import MetricNotesField from './MetricNotesField';

export const DatasetMetricsTable = ({ metrics }: DatasetMetricsProps) => {
    const [expandedRows, setExpandedRows] = useState({});
    const getDimensionName = (dimensionName: string): string => {
        //return toTitleCaseChangeCase(dimensionName.replace(/_/g, ' '));
        return (dimensionName.replace(/_/g, ' '));
    };

    const notesColumn = {
        title: 'Notes',
        dataIndex: 'note',
        key: 'note',
        render: (_, rec, index) => (
            <MetricNotesField
                onExpanded={(expanded) => {
                    setExpandedRows((prev) => ({ ...prev, [index]: expanded }));
                }}
                expanded={!!expandedRows[index]}
                notes={rec?.note || ''}
            />
        ),
    };

    const MetricsTable = ({ metrics }: DatasetMetricsProps): JSX.Element => {
        const columns = [
            {
                title: 'Quality Parameter',
                dataIndex: 'metric',
                key: 'metric',
                render: (_, record) => <> {getDimensionName(record?.metric)} </>,
                filterIcon: (filtered) => <div className={filtered ? 'FilterIconFiltered' : 'FilterIconUnfiltered'} />,
                filters: metrics?.map((rec) => ({
                    text: getDimensionName(rec?.metric?.toString()),
                    value: rec?.metric?.toString(),
                })),
                onFilter: (value: any, rec) => rec?.metric === value,
            },
            {
                title: 'Historical',
                dataIndex: 'historical',
                key: 'historical',
                sorter: (a, b) => a.historical.localeCompare(b.historical),
                render: (_, rec) => <>{getFormattedScore(rec?.scoreType, rec?.historical)}</>,
            },
            {
                title: 'Current',
                dataIndex: 'current',
                key: 'current',
                sorter: (a, b) => a.current.localeCompare(b.current),
                render: (_, rec) => <>{getFormattedScore(rec?.scoreType, rec?.current)}</>,
            },
            ...[notesColumn]
        ];

        return <Table dataSource={metrics} columns={columns} pagination={false} className="metrics-table" />;
    };

    return (
        <>
            <div className="MetricsTab">
                <br />
                <MetricsTable metrics={metrics} />
            </div>
        </>
    );
};
