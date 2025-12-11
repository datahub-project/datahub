/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import { getSourceUrnFromSchemaFieldUrn } from '@src/app/entityV2/schemaField/utils';

import { Entity, QueryEntity, SchemaFieldEntity } from '@types';

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
        createdBy: queryEntity?.properties?.createdOn?.actor,
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
