import { Empty } from 'antd';
import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import Query from './Query';
import { useBaseEntity } from '../../../EntityContext';
import getTopNQueries from './utils/getTopNQueries';
import { useAppConfig } from '../../../../../useAppConfig';

export default function QueriesTab() {
    const appConfig = useAppConfig();
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const topQueries = getTopNQueries(
        appConfig?.config?.visualConfig?.queriesTab?.queriesTabResultSize || 5,
        baseEntity?.dataset?.usageStats?.buckets,
    );

    if (topQueries.length === 0) {
        return <Empty description="No Sample Queries" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
    }

    return (
        <>
            {topQueries.map((query) => (
                <Query query={query} />
            ))}
        </>
    );
}
