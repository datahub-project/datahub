import { UsageAggregation, Maybe } from '../../../../../../../types.generated';

export default function getTopNQueries(responseSize: number, buckets?: Maybe<UsageAggregation>[] | null) {
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
