import { UsageAggregation, Maybe } from '../../../../../../../types.generated';

type RecentQuery = {
    query: string;
    dateMs: number;
};

export default function getTopNQueries(n: number, buckets?: Maybe<UsageAggregation>[] | null): RecentQuery[] {
    let response: RecentQuery[] = [];
    const seenQueries = new Set<string>();

    if (!buckets) {
        return response;
    }

    for (let i = 0; i < buckets.length; i++) {
        const bucket = buckets[i];
        const newQueries: string[] =
            (bucket?.metrics?.topSqlQueries?.filter(
                (query) => query && bucket?.bucket && !seenQueries.has(query),
            ) as string[]) || [];

        // Mark each query as seen.
        newQueries.forEach((query) => seenQueries.add(query as string));

        response = [...response, ...newQueries.map((query) => ({ query, dateMs: bucket?.bucket as number }))];

        if (response.length >= n) {
            return response.slice(0, n);
        }
    }

    return response;
}
