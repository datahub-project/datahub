/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Maybe, UsageAggregation } from '@types';

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
