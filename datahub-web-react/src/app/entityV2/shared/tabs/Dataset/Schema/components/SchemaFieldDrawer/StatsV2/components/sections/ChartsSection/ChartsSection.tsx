import React from 'react';
import NumericDistributionChart from './charts/NumericDistributionChart';
import NullValuesChart from './charts/NullValuesChart';
import DistinctValuesChart from './charts/DistinctValuesChart';
import MaxValuesChart from './charts/MaxValuesChart';
import MinValuesChart from './charts/MinValuesChart';
import MedianValuesChart from './charts/MedianValuesChart';
import MeanValuesChart from './charts/MeanValuesChart';
import RowCountByValueChart from './charts/RowCountByValueChart';

export default function ChartsSection() {
    return (
        <>
            <NullValuesChart />
            <DistinctValuesChart />
            <NumericDistributionChart />
            <MaxValuesChart />
            <MinValuesChart />
            <MedianValuesChart />
            <MeanValuesChart />
            <RowCountByValueChart />
        </>
    );
}
