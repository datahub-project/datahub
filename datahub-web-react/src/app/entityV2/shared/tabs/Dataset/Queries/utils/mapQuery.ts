import { getSourceUrnFromSchemaFieldUrn } from '@src/app/entityV2/schemaField/utils';
import { Entity, QueryEntity, SchemaFieldEntity } from '../../../../../../../types.generated';
import { Query } from '../types';

interface Props {
    queryEntity: QueryEntity;
    entityUrn?: string;
    siblingUrn?: string;
    poweredEntity?: Entity;
}

export function mapQuery({ queryEntity, entityUrn, siblingUrn, poweredEntity }: Props) {
    return {
        urn: queryEntity.urn,
        title: queryEntity.properties?.name || undefined,
        description: queryEntity.properties?.description || undefined,
        query: queryEntity.properties?.statement?.value || '',
        createdTime: queryEntity?.properties?.created?.time,
        columns: queryEntity.subjects
            ?.filter((s) => !!s.schemaField)
            ?.filter((s) => {
                const schemaFieldSourceUrn = getSourceUrnFromSchemaFieldUrn(s.schemaField?.urn || '');
                return schemaFieldSourceUrn === entityUrn || schemaFieldSourceUrn === siblingUrn;
            })
            .map((s) => s.schemaField) as SchemaFieldEntity[],
        poweredEntity,
    } as Query;
}
