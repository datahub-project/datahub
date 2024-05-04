import { GetChartQuery } from '../../../../graphql/chart.generated';
import { SchemaField } from '../../../../types.generated';
import { useBaseEntity } from '../../../entity/shared/EntityContext';

export function useGetTagFields(tag: string) {
    const chart = useBaseEntity<GetChartQuery>()?.chart;

    const fields = chart?.inputFields?.fields
        ?.filter((f) => f?.schemaField?.globalTags?.tags?.map((t) => t.tag.urn).includes(tag))
        .map((f) => f?.schemaField)
        .filter((f): f is SchemaField => !!f);
    return fields;
}
