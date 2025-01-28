import { GetChartQuery } from '../../../../graphql/chart.generated';
import { SchemaField } from '../../../../types.generated';
import { useBaseEntity } from '../../../entity/shared/EntityContext';

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
