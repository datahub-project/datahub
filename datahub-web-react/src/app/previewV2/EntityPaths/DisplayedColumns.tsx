import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components/macro';

import { decodeSchemaField } from '@app/lineageV3/utils/columnLineageUtils';
import { downgradeV2FieldPath } from '@app/lineageV3/utils/downgradeV2FieldPath';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType, SchemaFieldEntity } from '@types';

const ColumnNameWrapper = styled.span<{ isBlack?: boolean }>`
    font-weight: bold;
`;

const COLUMN_SEPARATOR = ', ';

interface Props {
    displayedColumns: (Maybe<Entity> | undefined)[];
}

export default function DisplayedColumns({ displayedColumns }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <span>
            {displayedColumns.map((entity, index) => {
                if (entity) {
                    const columnName =
                        entity.type === EntityType.SchemaField
                            ? decodeSchemaField(downgradeV2FieldPath((entity as SchemaFieldEntity).fieldPath) || '')
                            : entityRegistry.getDisplayName(entity.type, entity);
                    return (
                        <ColumnNameWrapper key={entity.urn} data-testid={`displayed-column-${columnName}`}>
                            {columnName}
                            {index !== displayedColumns.length - 1 && COLUMN_SEPARATOR}
                        </ColumnNameWrapper>
                    );
                }
                return null;
            })}
        </span>
    );
}
