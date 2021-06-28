import React from 'react';
import { Empty, Typography } from 'antd';
import styled from 'styled-components';

import { UsageQueryResult, Maybe, UsageAggregation } from '../../../../types.generated';
import Query from './Query';

export type Props = {
    usageStats?: UsageQueryResult | null;
};

const SectionTitle = styled(Typography.Title)`
    && {
        margin-bottom: 32px;
    }
`;

function getTopNQueries(responseSize: number, buckets?: Maybe<UsageAggregation>[] | null) {
    let response: (string | null)[] = [];
    if (!buckets) {
        return response;
    }

    for (let i = 0; i < buckets.length; i++) {
        const bucket = buckets[i];

        if (bucket?.metrics?.topSqlQueries && bucket?.metrics?.topSqlQueries !== null) {
            response = [...response, ...bucket?.metrics?.topSqlQueries];
            response = response.filter(Boolean);
            if (response.length >= responseSize) {
                return response.slice(0, responseSize);
            }
        }
    }

    return response;
}

export default function QueriesTab({ usageStats }: Props) {
    const topQueries = getTopNQueries(5, usageStats?.buckets);

    if (topQueries.length === 0) {
        return <Empty description="No Sample Queries" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
    }

    return (
        <>
            <SectionTitle level={3}>Recent Queries</SectionTitle>
            {topQueries.map((query) => (
                <Query query={query} />
            ))}
        </>
    );
}
