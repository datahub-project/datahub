import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { Entity, EntityType, SchemaFieldEntity } from '../../../../types.generated';
import { GenericEntityProperties } from '../types';

const PreviewImage = styled.img<{ size: number }>`
    height: ${(props) => props.size}px;
    width: ${(props) => props.size}px;
    min-width: ${(props) => props.size}px;
    object-fit: contain;
    background-color: transparent;
    margin: 0px 4px 0px 0px;
`;

const StyledLink = styled(Link)`
    margin-right: 4px;
    display: flex;
    align-items: center;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

interface Props {
    entity: Entity;
}

export default function PropagationEntityLink({ entity }: Props) {
    const entityRegistry = useEntityRegistry();

    const isSchemaField = entity.type === EntityType.SchemaField;
    const baseEntity = isSchemaField ? (entity as SchemaFieldEntity).parent : entity;

    const logoUrl = (baseEntity as GenericEntityProperties)?.platform?.properties?.logoUrl || '';
    let entityUrl = entityRegistry.getEntityUrl(baseEntity.type, baseEntity.urn);
    let entityDisplayName = entityRegistry.getDisplayName(baseEntity.type, baseEntity);

    if (isSchemaField) {
        entityUrl = `${entityUrl}/${encodeURIComponent('Columns')}?schemaFilter=${encodeURIComponent(
            (entity as SchemaFieldEntity).fieldPath,
        )}`;
        const schemaFieldName = entityRegistry.getDisplayName(entity.type, entity);
        entityDisplayName = `${entityDisplayName}.${schemaFieldName}`;
    }

    return (
        <>
            <StyledLink to={entityUrl}>
                <PreviewImage src={logoUrl} alt="test" size={14} />
                {entityDisplayName}
            </StyledLink>
        </>
    );
}
