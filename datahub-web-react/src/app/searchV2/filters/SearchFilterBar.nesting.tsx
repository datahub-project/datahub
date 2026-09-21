import React from 'react';

import { FilterValueOption } from '@components/components/FilterBar/types';

import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import GlossaryEntityIcon from '@app/glossaryV2/GlossaryEntityIcon';
import { getParentEntities } from '@app/searchV2/filters/utils';
import { extractParentDomains } from '@app/searchV2/filtersV2/filters/DomainFilter/utils';
import {
    DOMAINS_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    FIELD_GLOSSARY_TERMS_FILTER_NAME,
    FILTER_DELIMITER,
    GLOSSARY_TERMS_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import { EntityRegistry } from '@src/entityRegistryContext';

import { AggregationMetadata, Domain, Entity, EntityType, GlossaryNode, GlossaryTerm } from '@types';

const GLOSSARY_FILTER_FIELDS = new Set([GLOSSARY_TERMS_FILTER_NAME, FIELD_GLOSSARY_TERMS_FILTER_NAME]);

type FlatNestedOption = FilterValueOption & { parentValue?: string };

function getParentPathDescription(
    entity: Entity | null | undefined,
    entityRegistry: EntityRegistry,
): string | undefined {
    const parents = getParentEntities(entity as Entity);
    if (!parents?.length) return undefined;
    return [...parents]
        .reverse()
        .map((parent) => entityRegistry.getDisplayName(parent.type, parent))
        .join(' / ');
}

function buildTreeFromParentValues(options: FlatNestedOption[]): FilterValueOption[] {
    const byParent = new Map<string | undefined, FlatNestedOption[]>();
    options.forEach((option) => {
        const key = option.parentValue;
        byParent.set(key, [...(byParent.get(key) ?? []), option]);
    });

    const build = (parentValue?: string): FilterValueOption[] =>
        (byParent.get(parentValue) ?? []).map(({ parentValue: _parent, ...option }) => {
            const children = build(option.value);
            return children.length ? { ...option, children } : option;
        });

    return build(undefined);
}

function nestDomainOptions(
    options: FilterValueOption[],
    aggregations: AggregationMetadata[],
    entityRegistry: EntityRegistry,
): FilterValueOption[] {
    const domains = aggregations
        .map((aggregation) => aggregation.entity)
        .filter((entity): entity is Domain => !!entity && entity.type === EntityType.Domain);
    const parentDomains = extractParentDomains(domains);
    const optionByValue = new Map(options.map((option) => [option.value, option]));

    parentDomains.forEach((domain) => {
        if (optionByValue.has(domain.urn)) return;
        optionByValue.set(domain.urn, {
            value: domain.urn,
            label: domain.properties?.name || entityRegistry.getDisplayName(EntityType.Domain, domain),
            icon: <DomainColoredIcon domain={domain} size={20} fontSize={12} />,
        });
    });

    const flat: FlatNestedOption[] = Array.from(optionByValue.values()).map((option) => {
        const entity =
            domains.find((domain) => domain.urn === option.value) ||
            parentDomains.find((domain) => domain.urn === option.value);
        return {
            ...option,
            description: option.description || getParentPathDescription(entity, entityRegistry),
            parentValue: entity?.parentDomains?.domains?.[0]?.urn,
        };
    });

    return buildTreeFromParentValues(flat);
}

function nestGlossaryOptions(
    options: FilterValueOption[],
    aggregations: AggregationMetadata[],
    entityRegistry: EntityRegistry,
): FilterValueOption[] {
    const terms = aggregations
        .map((aggregation) => aggregation.entity)
        .filter((entity): entity is GlossaryTerm => !!entity && entity.type === EntityType.GlossaryTerm);

    const nodesByUrn = new Map<string, GlossaryNode>();
    terms.forEach((term) => {
        term.parentNodes?.nodes?.forEach((node) => {
            if (node?.urn) nodesByUrn.set(node.urn, node as GlossaryNode);
        });
    });

    const optionByValue = new Map(options.map((option) => [option.value, option]));
    nodesByUrn.forEach((node) => {
        if (optionByValue.has(node.urn)) return;
        // Term groups are structural parents in the glossaryTerms facet — expand only.
        optionByValue.set(node.urn, {
            value: node.urn,
            label: node.properties?.name || entityRegistry.getDisplayName(EntityType.GlossaryNode, node),
            icon: <GlossaryEntityIcon entity={node} size={20} iconSize={12} />,
            disabled: true,
        });
    });

    const flat: FlatNestedOption[] = Array.from(optionByValue.values()).map((option) => {
        const term = terms.find((candidate) => candidate.urn === option.value);
        const node = nodesByUrn.get(option.value);
        const entity = term || node;
        const directParent = term?.parentNodes?.nodes?.[0]?.urn || node?.parentNodes?.nodes?.[0]?.urn;
        return {
            ...option,
            description: option.description || getParentPathDescription(entity as Entity, entityRegistry),
            parentValue: directParent,
        };
    });

    return buildTreeFromParentValues(flat);
}

function nestEntitySubtypeOptions(options: FilterValueOption[]): FilterValueOption[] {
    const parents = options.filter((option) => !option.value.includes(FILTER_DELIMITER));
    const children = options.filter((option) => option.value.includes(FILTER_DELIMITER));
    if (!children.length) return options;

    return parents.map((parent) => {
        const nested = children.filter((child) => child.value.startsWith(`${parent.value}${FILTER_DELIMITER}`));
        return nested.length ? { ...parent, children: nested } : parent;
    });
}

/** Nest domain / glossary / entity-subtype value lists (same trees the old filter menus used). */
export function nestFilterBarOptions(
    fieldName: string,
    options: FilterValueOption[],
    aggregations: AggregationMetadata[],
    entityRegistry: EntityRegistry,
): FilterValueOption[] {
    if (fieldName === DOMAINS_FILTER_NAME) {
        return nestDomainOptions(options, aggregations, entityRegistry);
    }
    if (GLOSSARY_FILTER_FIELDS.has(fieldName)) {
        return nestGlossaryOptions(options, aggregations, entityRegistry);
    }
    if (fieldName === ENTITY_SUB_TYPE_FILTER_NAME) {
        return nestEntitySubtypeOptions(options);
    }
    return options.map((option) => {
        const entity = aggregations.find((aggregation) => aggregation.value === option.value)?.entity;
        const description = option.description || getParentPathDescription(entity, entityRegistry);
        return description ? { ...option, description } : option;
    });
}

export function flattenFilterBarOptions(options: FilterValueOption[]): FilterValueOption[] {
    return options.flatMap((option) => [option, ...flattenFilterBarOptions(option.children ?? [])]);
}
