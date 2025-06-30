import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components/macro';

import { downgradeV2FieldPath } from '@app/entity/dataset/profile/schema/utils/utils';
import { decodeSchemaField } from '@app/lineage/utils/columnLineageUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType, SchemaFieldEntity } from '@types';

const ColumnNameWrapper = styled.span<{ isBlack?: boolean }>`
    font-family: 'Roboto Mono', monospace;
    font-weight: bold;
    ${(props) => props.isBlack && 'color: black;'}
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
                        <ColumnNameWrapper>
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
