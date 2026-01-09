import React from 'react';

import DistinctValuesChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/DistinctValuesChart';
import MaxValuesChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/MaxValuesChart';
import MeanValuesChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/MeanValuesChart';
import MedianValuesChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/MedianValuesChart';
import MinValuesChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/MinValuesChart';
import NullValuesChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/NullValuesChart';
import NumericDistributionChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/NumericDistributionChart';
import RowCountByValueChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/RowCountByValueChart';

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
