import { DocumentCreator } from '@app/document/DocumentTreeContext';
import { capitalizeFirstLetterOnly } from '@app/sharedV2/textUtil';

import { CorpGroup, CorpUser, DataPlatform, Entity, EntityType, FacetMetadata } from '@types';

export type FacetSelectOption = {
    value: string;
    label: string;
    entity?: Entity;
};

export type AuthorFacetOption = FacetSelectOption & {
    creator: DocumentCreator;
};

export const AI_AGENT_URN_PREFIX = 'urn:li:aiAgent:';

export function isDataPlatformEntity(entity?: Entity | null): entity is DataPlatform {
    return entity?.type === EntityType.DataPlatform;
}

export function isAiAgentUrn(urn: string): boolean {
    return urn.startsWith(AI_AGENT_URN_PREFIX);
}

/** Drop AI-agent creators from Author options (OSS — agents aren't first-class UI actors). */
export function filterOutAiAgentAuthors<T extends { value: string }>(options: T[]): T[] {
    return options.filter((option) => !isAiAgentUrn(option.value));
}

export function resolveCreatorFromEntity(entity: Entity, displayName: string): DocumentCreator | null {
    if (entity.type === EntityType.CorpUser) {
        const user = entity as CorpUser;
        return {
            urn: user.urn,
            type: EntityType.CorpUser,
            displayName,
            pictureLink: user.editableProperties?.pictureLink ?? null,
        };
    }
    if (entity.type === EntityType.CorpGroup) {
        const group = entity as CorpGroup;
        return {
            urn: group.urn,
            type: EntityType.CorpGroup,
            displayName,
            pictureLink: null,
        };
    }
    return null;
}

export function mapFacetToEntityOptions(
    facet: FacetMetadata | undefined,
    getDisplayName: (type: EntityType, entity: Entity) => string,
): FacetSelectOption[] {
    return (facet?.aggregations ?? [])
        .filter((aggregation) => aggregation.count > 0 && !!aggregation.value)
        .map((aggregation) => ({
            value: aggregation.value,
            label: aggregation.entity ? getDisplayName(aggregation.entity.type, aggregation.entity) : aggregation.value,
            entity: aggregation.entity ?? undefined,
        }));
}

export function mapFacetToTypeOptions(facet: FacetMetadata | undefined): FacetSelectOption[] {
    return (facet?.aggregations ?? [])
        .filter((aggregation) => aggregation.count > 0 && !!aggregation.value)
        .map((aggregation) => ({
            value: aggregation.value,
            label: capitalizeFirstLetterOnly(aggregation.value) || aggregation.value,
        }));
}

export function mapFacetToAuthorOptions(
    facet: FacetMetadata | undefined,
    getDisplayName: (type: EntityType, entity: Entity) => string,
): AuthorFacetOption[] {
    const options: AuthorFacetOption[] = [];
    (facet?.aggregations ?? []).forEach((aggregation) => {
        if (aggregation.count <= 0 || !aggregation.value || !aggregation.entity) return;
        const { entity } = aggregation;
        const displayName = getDisplayName(entity.type, entity);
        const creator = resolveCreatorFromEntity(entity, displayName);
        if (!creator) return;
        options.push({
            value: aggregation.value,
            label: displayName,
            entity,
            creator,
        });
    });
    return options;
}

/** Selected values that the facet no longer returns (e.g. after a search narrowed the results). */
export function getMissingSelectedValues(options: FacetSelectOption[], selected: string[]): string[] {
    const present = new Set(options.map((o) => o.value));
    return selected.filter((value) => !present.has(value));
}

/**
 * Keep selected values in the dropdown even when their agg count is 0 (search parity). `resolved` holds options for
 * values the facet no longer returns, so they keep their names; without one the raw value (a urn) is the label.
 */
export function ensureSelectedOptions(
    options: FacetSelectOption[],
    selected: string[],
    resolved?: Map<string, FacetSelectOption>,
): FacetSelectOption[] {
    const missing = getMissingSelectedValues(options, selected).map(
        (value) => resolved?.get(value) ?? { value, label: value },
    );
    return missing.length === 0 ? options : [...options, ...missing];
}

export function ensureSelectedAuthorOptions(
    options: AuthorFacetOption[],
    selected: string[],
    resolved?: Map<string, FacetSelectOption>,
): AuthorFacetOption[] {
    const missing = getMissingSelectedValues(options, selected).map((value) => {
        const resolvedOption = resolved?.get(value);
        const displayName = resolvedOption?.label ?? value;
        return {
            value,
            label: displayName,
            entity: resolvedOption?.entity,
            creator: {
                urn: value,
                type: EntityType.CorpUser,
                displayName,
                pictureLink: null,
            },
        };
    });
    return missing.length === 0 ? options : [...options, ...missing];
}
