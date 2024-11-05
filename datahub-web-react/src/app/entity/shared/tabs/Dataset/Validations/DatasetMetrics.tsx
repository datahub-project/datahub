import React, { useState } from 'react';
import './MetricsTab.less';
import { Card, Radio } from 'antd';
import { DatasetMetricsChart } from './DatasetMetricsChart';
import { DatasetMetricsTable } from './DatasetMetricsTable';
import { DatasetMetricsProps } from './Metrics';
import Icon from '../../../../../../images/Icon.png';

export const DatasetMetrics = ({ metrics, title }: DatasetMetricsProps) => {
    const CHART = 'Chart';
    const TABLE = 'Table';
    const [selectedOption, setSelectedOption] = useState(CHART);
    const hasDatasetMetrics = metrics && metrics?.length > 0;

    const changeView = (e) => {
        setSelectedOption(e.target.value);
    };
    return (
        <div className="MetricsTab">
            <p className="section-heading">Dataset Metrics</p>
            <p className="section-sub-heading">{title}</p>
            {!hasDatasetMetrics && (
                <Card className="error-card">
                    <img className="card-error-img" src={Icon} alt="filter" />
                    <span className="error-text">There are no dataset metrics available for this dataset.</span>
                </Card>
            )}
            {hasDatasetMetrics && (
                <>
                    <Radio.Group defaultValue="Chart" buttonStyle="solid">
                        <Radio.Button value={CHART} className="radioButtonLeft" onClick={changeView}>
                            Chart
                        </Radio.Button>
                        <Radio.Button value={TABLE} className="radioButtonRight" onClick={changeView}>
                            Table
                        </Radio.Button>
                    </Radio.Group>
                    {selectedOption === CHART && <DatasetMetricsChart metrics={metrics} />}
                    {selectedOption === TABLE && <DatasetMetricsTable metrics={metrics} />}
                </>
            )}
        </div>
    );
};
