import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import type { GenericEntityProperties } from '@app/entity/shared/types';
import { getSchemaFieldParentLink, getSourceUrnFromSchemaFieldUrn } from '@app/entityV2/schemaField/utils';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';
import { getV1FieldPathFromSchemaFieldUrn } from '@app/lineageV3/utils/lineageUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { Entity } from '@types';

const SCHEMA_FIELD_PREFIX = 'urn:li:schemaField:';
const DATASET_URN_PREFIX = 'urn:li:dataset:';

const ReplacementContainer = styled.span`
    display: inline-block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    // make sure the span doesn't exceed the parent div
    max-width: 100%;
`;

const ReplacementLink = styled(Link)`
    color: ${(props) => props.theme.colors.textSecondary};
    display: inline-block;
    max-width: 100%;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;

    &:hover {
        color: ${(props) => props.theme.colors.textBrand};
    }
`;

type Props = {
    replacement: Entity;
    /**
     * The parent lookup waits for the popover to have been opened, since the icon renders once per
     * row of a schema table. It does not go back to waiting on close: the label would drop the
     * parent name for as long as the popover takes to animate away.
     */
    hasPopoverOpened: boolean;
};

/**
 * The replacement of a deprecation. An asset gets the standard entity link; a column is spelled out
 * as "<parent>.<field path>", because a field path on its own is ambiguous once the replacement can
 * live in a different asset than the deprecated column.
 */
export default function DeprecationReplacement({ replacement, hasPopoverOpened }: Props) {
    const entityRegistry = useEntityRegistry();
    const columnUrn = replacement.urn?.startsWith(SCHEMA_FIELD_PREFIX) ? replacement.urn : undefined;

    // The urn carries the parent, but the deprecation aspect doesn't resolve it, hence the lookup —
    // skipped entirely for the far more common asset replacement.
    const parentUrn = columnUrn ? getSourceUrnFromSchemaFieldUrn(columnUrn) : undefined;
    const { data: parentData } = useGetEntitiesQuery({
        variables: {
            urns: [parentUrn || ''],
            // Only the parent's name is read here, so its lineage counts and sibling search — both
            // graph queries — are left out.
            skipLineage: true,
            skipSiblingsSearch: true,
        },
        skip: !parentUrn || !hasPopoverOpened,
        fetchPolicy: 'cache-first',
    });
    // The generated query type narrows nested aspects to the fragment's selections, which no longer
    // structurally match the full types GenericEntityProperties is built from.
    const parent = parentData?.entities?.[0] as GenericEntityProperties | undefined;

    if (!columnUrn) return <EntityLink entity={replacement} />;

    const parentName = parent?.type && entityRegistry.getDisplayName(parent.type, parent);
    const label = [parentName, getV1FieldPathFromSchemaFieldUrn(columnUrn)].filter(Boolean).join('.');
    // getSchemaFieldParentLink only knows the dataset route, and a glossary term carries
    // schemaMetadata too, so a column on anything else is shown unlinked rather than linked nowhere.
    if (parentUrn?.startsWith(DATASET_URN_PREFIX)) {
        return <ReplacementLink to={getSchemaFieldParentLink(columnUrn)}>{label}</ReplacementLink>;
    }
    return <ReplacementContainer>{label}</ReplacementContainer>;
}
