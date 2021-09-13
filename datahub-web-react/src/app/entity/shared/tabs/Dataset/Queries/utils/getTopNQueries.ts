import { UsageAggregation, Maybe } from '../../../../../../../types.generated';

export default function getTopNQueries(responseSize: number, buckets?: Maybe<UsageAggregation>[] | null) {
    let response: (string | null)[] = [];
    if (!buckets) {
        return response;
    }

    const unique = (value: string | null, index: number, self): boolean => {
        return self.indexOf(value) === index;
    };

    for (let i = 0; i < buckets.length; i++) {
        const bucket = buckets[i];

        if (bucket?.metrics?.topSqlQueries && bucket?.metrics?.topSqlQueries !== null) {
            response = [...response, ...bucket?.metrics?.topSqlQueries];
            response = response.filter(Boolean).filter(unique);
            if (response.length >= responseSize) {
                return response.slice(0, responseSize);
            }
        }
    }

    return response;
}
