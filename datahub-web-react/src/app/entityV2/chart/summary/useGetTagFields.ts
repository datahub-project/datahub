/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useBaseEntity } from '@app/entity/shared/EntityContext';

import { GetChartQuery } from '@graphql/chart.generated';
import { SchemaField } from '@types';

export function useGetTagFields(tag: string): SchemaField[] | undefined {
    const chart = useBaseEntity<GetChartQuery>()?.chart;

    // Have to type cast because of line `businessAttributeDataType: type` in `businessAttribute` fragment
    // Can we get rid of this? Why is this renamed?
    return chart?.inputFields?.fields
        ?.filter((f) =>
            f?.schemaField?.globalTags?.tags?.map((t) => t.tag.urn.toLowerCase()).includes(tag.toLowerCase()),
        )
        .map((f) => f?.schemaField)
        .filter((f) => !!f) as SchemaField[] | undefined;
}
