import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components/macro';
import { Entity, EntityType, SchemaFieldEntity } from '../../../types.generated';
import { downgradeV2FieldPath } from '../../entity/dataset/profile/schema/utils/utils';
import { decodeSchemaField } from '../../lineage/utils/columnLineageUtils';
import { useEntityRegistry } from '../../useEntityRegistry';

const ColumnNameWrapper = styled.span<{ isBlack?: boolean }>`
    font-weight: bold;
`;

interface Props {
    displayedColumns: (Maybe<Entity> | undefined)[];
}

export default function DisplayedColumns({ displayedColumns }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <span>
            {displayedColumns.map((entity, index) => {
                if (entity) {
                    return (
                        <ColumnNameWrapper key={entity.urn}>
                            {entity.type === EntityType.SchemaField
                                ? decodeSchemaField(downgradeV2FieldPath((entity as SchemaFieldEntity).fieldPath) || '')
                                : entityRegistry.getDisplayName(entity.type, entity)}
                            {index !== displayedColumns.length - 1 && ', '}
                        </ColumnNameWrapper>
                    );
                }
                return null;
            })}
        </span>
    );
}
