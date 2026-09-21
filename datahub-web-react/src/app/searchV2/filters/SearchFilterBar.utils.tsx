import { Avatar, Input } from '@components';
import i18next from 'i18next';
import React, { useState } from 'react';

import { FilterPopover } from '@components/components/FilterBar/components';
import {
    FilterField,
    FilterGroup,
    FilterOperator,
    FilterRule,
    FilterValueEditorProps,
    FilterValueOption,
} from '@components/components/FilterBar/types';

import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import { isCorpUser } from '@app/entityV2/user/utils';
import GlossaryEntityIcon from '@app/glossaryV2/GlossaryEntityIcon';
import { TagColor } from '@app/searchV2/filters/FilterOption';
import {
    DateRangeFilterValueEditor,
    EntityFilterValueEditor,
    booleanFilterOptions,
} from '@app/searchV2/filters/SearchFilterBar.editors';
import { nestFilterBarOptions } from '@app/searchV2/filters/SearchFilterBar.nesting';
import { FILTERS_TO_REMOVE, SORTED_FILTERS } from '@app/searchV2/filters/constants';
import { ALL_FILTER_FIELDS, DEFAULT_FILTER_FIELDS, LAST_MODIFIED_FILTER } from '@app/searchV2/filters/field/fields';
import {
    FieldType,
    FrontendFilterOperator,
    FilterField as SearchFilterField,
    TimeBucketFilterField,
} from '@app/searchV2/filters/types';
import {
    PlatformIcon,
    filterEmptyAggregations,
    getDynamicFilterField,
    getFilterIconAndLabel,
    getIsDateRangeFilter,
    getParentEntities,
    sortFacets,
} from '@app/searchV2/filters/utils';
import {
    CONTAINER_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    FIELD_GLOSSARY_TERMS_FILTER_NAME,
    FIELD_TAGS_FILTER_NAME,
    FIELD_TO_LABEL,
    GLOSSARY_TERMS_FILTER_NAME,
    LAST_MODIFIED_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PARENT_DOCUMENT_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
    TEXT_FIELDS,
    UnionType,
} from '@app/searchV2/utils/constants';
import { EntityRegistry } from '@src/entityRegistryContext';
import dayjs from '@utils/dayjs';

import {
    AggregationMetadata,
    FilterOperator as BackendFilterOperator,
    CorpUser,
    DataPlatform,
    Domain,
    Entity,
    EntityType,
    FacetFilterInput,
    FacetMetadata,
    GlossaryNode,
    GlossaryTerm,
    Tag,
} from '@types';

/** Primary search filters — matches SORTED_FILTERS / filtersV2 priority. */
export const DEFAULT_SEARCH_FILTER_FIELDS = [ENTITY_SUB_TYPE_FILTER_NAME, PLATFORM_FILTER_NAME, OWNERS_FILTER_NAME];

const HIERARCHICAL_FIELDS = new Set([DOMAINS_FILTER_NAME, CONTAINER_FILTER_NAME, PARENT_DOCUMENT_FILTER_NAME]);

const GLOSSARY_FILTER_FIELDS = new Set([GLOSSARY_TERMS_FILTER_NAME, FIELD_GLOSSARY_TERMS_FILTER_NAME]);

/** Fields where "is all of" is meaningful (multi-valued aspects). Not Type / Platform. */
const ALL_OF_FIELDS = new Set([
    OWNERS_FILTER_NAME,
    TAGS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    FIELD_TAGS_FILTER_NAME,
    FIELD_GLOSSARY_TERMS_FILTER_NAME,
    DOMAINS_FILTER_NAME,
]);

const ENTITY_OPERATORS: FilterOperator[] = [
    { value: 'is', label: 'is', pluralLabel: 'is any of' },
    { value: 'is_all', label: 'is all of' },
    { value: 'is_not', label: 'is not', pluralLabel: 'is not any of' },
    { value: 'exists', label: 'exists', requiresValue: false },
    { value: 'not_exists', label: 'does not exist', requiresValue: false },
];

const HIERARCHICAL_OPERATORS: FilterOperator[] = [
    { value: 'within', label: 'within' },
    { value: 'is', label: 'is', pluralLabel: 'is any of' },
    { value: 'is_all', label: 'is all of' },
    { value: 'is_not', label: 'is not', pluralLabel: 'is not any of' },
    { value: 'exists', label: 'exists', requiresValue: false },
    { value: 'not_exists', label: 'does not exist', requiresValue: false },
];

const TEXT_OPERATORS: FilterOperator[] = [
    {
        value: 'contains',
        label: 'contains',
        pluralLabel: 'contains any of',
    },
    {
        value: 'not_contains',
        label: 'does not contain',
        pluralLabel: 'does not contain any of',
    },
    { value: 'is', label: 'is', pluralLabel: 'is any of' },
    { value: 'is_not', label: 'is not', pluralLabel: 'is not any of' },
    { value: 'exists', label: 'exists', requiresValue: false },
    { value: 'not_exists', label: 'does not exist', requiresValue: false },
];

const BOOLEAN_OPERATORS: FilterOperator[] = [
    { value: 'is', label: 'is' },
    { value: 'is_not', label: 'is not' },
];

const SINCE_OPERATORS: FilterOperator[] = [{ value: 'since', label: 'since' }];

const BETWEEN_OPERATORS: FilterOperator[] = [{ value: 'between', label: 'between' }];

function withoutAllOf(operators: FilterOperator[]): FilterOperator[] {
    return operators.filter((operator) => operator.value !== 'is_all');
}

/** Prefix for last-modified preset offsets stored on the chip before converting to a timestamp. */
export const LAST_MODIFIED_OFFSET_PREFIX = 'lm-offset:';

const DAY_IN_MILLIS = 24 * 60 * 60 * 1000;

function TextValueEditor({ rule, onChange, trigger, compose }: FilterValueEditorProps) {
    const isCompose = !!compose;
    const [isOpen, setIsOpen] = useState(false);

    const input = (
        <Input
            value={rule.values[0] ?? ''}
            setValue={(value) => onChange({ ...rule, values: value ? [value] : [] })}
            placeholder={i18next.t('search:filters.enterValue', { name: 'value' })}
            onClear={() => onChange({ ...rule, values: [] })}
        />
    );

    if (isCompose) return input;

    return (
        <FilterPopover
            isOpen={isOpen}
            onClose={() => setIsOpen(false)}
            trigger={React.cloneElement(trigger, { onClick: () => setIsOpen(!isOpen) })}
        >
            {input}
        </FilterPopover>
    );
}

function getSearchFilterField(fieldName: string, availableFilters: FacetMetadata[]): SearchFilterField {
    return (
        ALL_FILTER_FIELDS.find((field) => field.field === fieldName) ||
        getDynamicFilterField(fieldName, availableFilters)
    );
}

function getOperatorsForField(field: SearchFilterField): FilterOperator[] {
    if (field.field === LAST_MODIFIED_FILTER_NAME || field.type === FieldType.BUCKETED_TIMESTAMP) {
        return SINCE_OPERATORS;
    }
    if (getIsDateRangeFilter(field)) return BETWEEN_OPERATORS;
    if (HIERARCHICAL_FIELDS.has(field.field)) {
        return ALL_OF_FIELDS.has(field.field) ? HIERARCHICAL_OPERATORS : withoutAllOf(HIERARCHICAL_OPERATORS);
    }
    if (field.type === FieldType.BOOLEAN) return BOOLEAN_OPERATORS;
    if (field.type === FieldType.TEXT || TEXT_FIELDS.has(field.field)) return TEXT_OPERATORS;
    return ALL_OF_FIELDS.has(field.field) ? ENTITY_OPERATORS : withoutAllOf(ENTITY_OPERATORS);
}

function getDefaultOperator(field: SearchFilterField): string {
    if (field.field === LAST_MODIFIED_FILTER_NAME || field.type === FieldType.BUCKETED_TIMESTAMP) return 'since';
    if (getIsDateRangeFilter(field)) return 'between';
    if (HIERARCHICAL_FIELDS.has(field.field)) return 'within';
    if (field.type === FieldType.TEXT || TEXT_FIELDS.has(field.field)) return 'contains';
    return 'is';
}

function lastModifiedBucketOptions(): FilterValueOption[] {
    return (LAST_MODIFIED_FILTER as TimeBucketFilterField).options.map((option) => ({
        value: `${LAST_MODIFIED_OFFSET_PREFIX}${option.startOffsetMillis}`,
        label: option.label,
    }));
}

function matchLastModifiedToBucket(timestamp: string): { value: string; label: string } {
    const ts = Number(timestamp);
    if (Number.isNaN(ts)) return { value: timestamp, label: timestamp };
    const buckets = (LAST_MODIFIED_FILTER as TimeBucketFilterField).options;
    const match = buckets.find((option) => {
        const expected = dayjs().subtract(option.startOffsetMillis, 'milliseconds').startOf('day').valueOf();
        return Math.abs(expected - ts) < DAY_IN_MILLIS;
    });
    if (match) {
        return { value: `${LAST_MODIFIED_OFFSET_PREFIX}${match.startOffsetMillis}`, label: match.label };
    }
    return { value: timestamp, label: dayjs(ts).format('ll') };
}

function offsetValueToTimestamp(value: string): string {
    if (!value.startsWith(LAST_MODIFIED_OFFSET_PREFIX)) return value;
    const offset = Number(value.slice(LAST_MODIFIED_OFFSET_PREFIX.length));
    if (Number.isNaN(offset)) return value;
    return dayjs().subtract(offset, 'milliseconds').startOf('day').valueOf().toString();
}

function getValueIcon(
    fieldName: string,
    value: string,
    entity: Entity | null | undefined,
    entityRegistry: EntityRegistry,
): React.ReactNode | undefined {
    if (fieldName === TAGS_FILTER_NAME && entity?.type === EntityType.Tag) {
        return <TagColor color={(entity as Tag).properties?.colorHex || ''} colorHash={entity.urn} />;
    }
    if (fieldName === DOMAINS_FILTER_NAME && entity?.type === EntityType.Domain) {
        return <DomainColoredIcon domain={entity as Domain} size={20} fontSize={12} />;
    }
    if (
        GLOSSARY_FILTER_FIELDS.has(fieldName) &&
        (entity?.type === EntityType.GlossaryTerm || entity?.type === EntityType.GlossaryNode)
    ) {
        return <GlossaryEntityIcon entity={entity as GlossaryTerm | GlossaryNode} size={20} iconSize={12} />;
    }
    if (fieldName === OWNERS_FILTER_NAME && isCorpUser(entity)) {
        const user = entity as CorpUser;
        return (
            <Avatar
                name={entityRegistry.getDisplayName(EntityType.CorpUser, user)}
                imageUrl={user.editableProperties?.pictureLink || undefined}
                size="sm"
            />
        );
    }
    if (fieldName === PLATFORM_FILTER_NAME) {
        const logoUrl = (entity as DataPlatform | null)?.properties?.logoUrl;
        if (logoUrl) return <PlatformIcon src={logoUrl} size={16} />;
    }

    const { icon } = getFilterIconAndLabel(fieldName, value, entityRegistry, entity || null, 16);
    return icon || undefined;
}

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

function aggregationToOption(
    fieldName: string,
    aggregation: AggregationMetadata,
    entityRegistry: EntityRegistry,
): FilterValueOption {
    const { label } = getFilterIconAndLabel(
        fieldName,
        aggregation.value,
        entityRegistry,
        aggregation.entity || null,
        16,
    );
    return {
        value: aggregation.value,
        label: label || aggregation.value,
        count: aggregation.count,
        icon: getValueIcon(fieldName, aggregation.value, aggregation.entity, entityRegistry),
        description: getParentPathDescription(aggregation.entity, entityRegistry),
    };
}

/** Same priority as useFilterDisplayName — entity display name before raw facet field keys. */
function getFilterBarFieldLabel(
    facet: FacetMetadata,
    searchField: SearchFilterField,
    entityRegistry: EntityRegistry,
): string {
    const entity = facet.entity || searchField.entity;
    if (entity) {
        return entityRegistry.getDisplayName(entity.type, entity);
    }
    return FIELD_TO_LABEL[facet.field] || searchField.displayName || facet.displayName || facet.field;
}

function compareLabels(left: string, right: string): number {
    return left.localeCompare(right, undefined, { sensitivity: 'base' });
}

function sortValueOptionsAlphabetically(options: FilterValueOption[]): FilterValueOption[] {
    return [...options]
        .map((option) =>
            option.children?.length ? { ...option, children: sortValueOptionsAlphabetically(option.children) } : option,
        )
        .sort((left, right) => compareLabels(left.label, right.label));
}

export function buildFilterBarFields(
    availableFilters: FacetMetadata[],
    activeFilters: FacetFilterInput[],
    entityRegistry: EntityRegistry,
    options?: { isContextDocumentsEnabled?: boolean },
): FilterField[] {
    // Facets from search, minus legacy / browse-only fields.
    const facetsFromSearch = availableFilters
        .filter((facet) => !FILTERS_TO_REMOVE.includes(facet.field))
        .filter((facet) => (facet.field === ORIGIN_FILTER_NAME ? facet.aggregations.length >= 2 : true));

    const existingFields = new Set(facetsFromSearch.map((facet) => facet.field));

    // Always-available filters from the old Add Filter list (Last Modified, Container, Data Product, …)
    // even when the backend didn't return a facet for them.
    const alwaysAvailable = DEFAULT_FILTER_FIELDS.filter((filterField) => {
        if (filterField.field === PARENT_DOCUMENT_FILTER_NAME && !options?.isContextDocumentsEnabled) {
            return false;
        }
        // Origin only when the live facet says there are 2+ environments.
        if (filterField.field === ORIGIN_FILTER_NAME) return false;
        return !existingFields.has(filterField.field);
    }).map(
        ({ field, displayName }): FacetMetadata => ({
            field,
            displayName,
            aggregations: [],
        }),
    );

    const filterSet = [...facetsFromSearch, ...alwaysAvailable].sort((left, right) =>
        sortFacets(left, right, SORTED_FILTERS),
    );

    return filterSet.map((facet) => {
        const searchField = getSearchFilterField(facet.field, availableFilters);
        const activeForField = activeFilters.filter((filter) => filter.field === facet.field);
        const aggregations = filterEmptyAggregations(facet.aggregations || [], activeFilters);
        const flatValues = aggregations.map((aggregation) =>
            aggregationToOption(facet.field, aggregation, entityRegistry),
        );
        const values = sortValueOptionsAlphabetically(
            nestFilterBarOptions(facet.field, flatValues, aggregations, entityRegistry),
        );

        const selectedOptions =
            activeForField
                .flatMap((filter) => filter.values || [])
                .filter((value) => !flatValues.some((option) => option.value === value))
                .map((value) => {
                    if (facet.field === LAST_MODIFIED_FILTER_NAME) {
                        const matched = matchLastModifiedToBucket(value);
                        return { value: matched.value, label: matched.label };
                    }
                    const entity =
                        availableFilters
                            .find((candidate) => candidate.field === facet.field)
                            ?.aggregations.find((aggregation) => aggregation.value === value)?.entity || null;
                    const { label } = getFilterIconAndLabel(facet.field, value, entityRegistry, entity, 16);
                    return {
                        value,
                        label: label || value,
                        icon: getValueIcon(facet.field, value, entity, entityRegistry),
                    };
                }) ?? [];

        const isText = searchField.type === FieldType.TEXT || TEXT_FIELDS.has(facet.field);
        const isBoolean = searchField.type === FieldType.BOOLEAN;
        const isLastModified =
            facet.field === LAST_MODIFIED_FILTER_NAME || searchField.type === FieldType.BUCKETED_TIMESTAMP;
        const isDateRange = getIsDateRangeFilter(searchField);
        // Always load live aggregations for facet enums/entities. Search-response facets are
        // narrowed by the current filters — once "Tag is kpi" is applied they'd only show tags
        // on those results. useLoadAggregationOptions excludes this field so other values remain.
        const useLiveAggregationEditor = !isText && !isBoolean && !isLastModified && !isDateRange;

        let fieldValues: FilterValueOption[] | undefined = values;
        if (isBoolean) fieldValues = booleanFilterOptions();
        if (isLastModified) fieldValues = lastModifiedBucketOptions();
        if (isText || isDateRange) fieldValues = undefined;

        const renderValueEditor = (props: FilterValueEditorProps) => {
            if (isText) return <TextValueEditor {...props} />;
            if (isDateRange) return <DateRangeFilterValueEditor {...props} />;
            if (useLiveAggregationEditor) {
                return <EntityFilterValueEditor {...props} searchField={searchField} />;
            }
            return null;
        };

        return {
            field: facet.field,
            label: getFilterBarFieldLabel(facet, searchField, entityRegistry),
            operators: getOperatorsForField(searchField),
            defaultOperator: getDefaultOperator(searchField),
            values: fieldValues,
            selectedOptions: selectedOptions.length ? selectedOptions : undefined,
            selectionMode: isBoolean || isLastModified || isDateRange ? 'single' : 'multiple',
            searchable: !isBoolean && !isText && !isLastModified && !isDateRange,
            showSelectAll: !isBoolean && !isText && !isLastModified && !isDateRange,
            renderValueEditor: isText || isDateRange || useLiveAggregationEditor ? renderValueEditor : undefined,
        };
    });
}

function backendFilterToOperator(filter: FacetFilterInput, field: SearchFilterField): string {
    if (filter.condition === BackendFilterOperator.Exists) {
        return filter.negated ? 'not_exists' : 'exists';
    }
    if (filter.condition === BackendFilterOperator.Contain) {
        return filter.negated ? 'not_contains' : 'contains';
    }
    if (filter.condition === BackendFilterOperator.DescendantsIncl) {
        return 'within';
    }
    if (filter.condition === BackendFilterOperator.GreaterThan) {
        return field.field === LAST_MODIFIED_FILTER_NAME ? 'since' : 'between';
    }
    if (filter.condition === BackendFilterOperator.LessThan) {
        return 'between';
    }
    // Frontend-only AllEqual — AND of each selected value within the chip.
    if ((filter.condition as string) === FrontendFilterOperator.AllEqual) {
        return 'is_all';
    }
    if (filter.negated) return 'is_not';
    if (field.type === FieldType.TEXT || TEXT_FIELDS.has(field.field)) {
        return 'contains';
    }
    return 'is';
}

export function activeFiltersToFilterGroup(
    activeFilters: FacetFilterInput[],
    unionType: UnionType,
    availableFilters: FacetMetadata[],
    draftRules: FilterRule[] = [],
): FilterGroup {
    const usableFilters = activeFilters.filter((filter) => !FILTERS_TO_REMOVE.includes(filter.field));

    // Pair GreaterThan + LessThan on the same field into a single `between` chip (date range).
    const consumed = new Set<number>();
    const appliedRules: FilterRule[] = [];

    usableFilters.forEach((filter, index) => {
        if (consumed.has(index)) return;

        if (filter.condition === BackendFilterOperator.GreaterThan) {
            const endIndex = usableFilters.findIndex(
                (candidate, candidateIndex) =>
                    candidateIndex !== index &&
                    !consumed.has(candidateIndex) &&
                    candidate.field === filter.field &&
                    candidate.condition === BackendFilterOperator.LessThan,
            );
            if (endIndex >= 0) {
                consumed.add(index);
                consumed.add(endIndex);
                const start = filter.values?.[0] || '';
                const end = usableFilters[endIndex].values?.[0] || '';
                appliedRules.push({
                    id: `applied-${filter.field}-between-${appliedRules.length}`,
                    field: filter.field,
                    operator: 'between',
                    values: [start, end].filter(Boolean),
                });
                return;
            }

            if (filter.field === LAST_MODIFIED_FILTER_NAME && filter.values?.[0]) {
                consumed.add(index);
                const matched = matchLastModifiedToBucket(filter.values[0]);
                appliedRules.push({
                    id: `applied-${filter.field}-since-${appliedRules.length}`,
                    field: filter.field,
                    operator: 'since',
                    values: [matched.value],
                });
                return;
            }
        }

        if (filter.condition === BackendFilterOperator.LessThan && filter.field === LAST_MODIFIED_FILTER_NAME) {
            // Orphan end-bound without start — skip; between handler consumes pairs.
            const hasStart = usableFilters.some(
                (candidate, candidateIndex) =>
                    candidateIndex !== index &&
                    candidate.field === filter.field &&
                    candidate.condition === BackendFilterOperator.GreaterThan,
            );
            if (hasStart) return;
        }

        const searchField = getSearchFilterField(filter.field, availableFilters);
        appliedRules.push({
            id: `applied-${filter.field}-${filter.condition || 'EQUAL'}-${!!filter.negated}-${appliedRules.length}`,
            field: filter.field,
            operator: backendFilterToOperator(filter, searchField),
            values: filter.values || [],
        });
    });

    const appliedIds = new Set(appliedRules.map((rule) => rule.id));
    const appliedFields = new Set(appliedRules.map((rule) => rule.field));

    const pendingRules = draftRules.filter((rule) => {
        if (appliedIds.has(rule.id)) return false;
        if (isEmptyDefaultDraft(rule) && appliedFields.has(rule.field)) return false;
        return true;
    });

    return {
        id: 'search-root',
        match: unionType === UnionType.OR ? 'any' : 'all',
        filters: [...appliedRules, ...pendingRules],
    };
}

function isEmptyDefaultDraft(rule: FilterRule): boolean {
    return rule.id.startsWith('default-') && rule.values.length === 0;
}

function ruleToFacetFilters(rule: FilterRule): FacetFilterInput[] {
    switch (rule.operator) {
        case 'since': {
            const timestamp = offsetValueToTimestamp(rule.values[0] || '');
            if (!timestamp) return [];
            return [
                {
                    field: rule.field,
                    values: [timestamp],
                    condition: BackendFilterOperator.GreaterThan,
                    negated: false,
                },
            ];
        }
        case 'between': {
            const [start, end] = rule.values;
            const filters: FacetFilterInput[] = [];
            if (start) {
                filters.push({
                    field: rule.field,
                    values: [start],
                    condition: BackendFilterOperator.GreaterThan,
                    negated: false,
                });
            }
            if (end) {
                filters.push({
                    field: rule.field,
                    values: [end],
                    condition: BackendFilterOperator.LessThan,
                    negated: false,
                });
            }
            return filters;
        }
        case 'exists':
            return [{ field: rule.field, values: [], condition: BackendFilterOperator.Exists, negated: false }];
        case 'not_exists':
            return [{ field: rule.field, values: [], condition: BackendFilterOperator.Exists, negated: true }];
        case 'contains':
            return [
                {
                    field: rule.field,
                    values: rule.values,
                    condition: BackendFilterOperator.Contain,
                    negated: false,
                },
            ];
        case 'not_contains':
            return [
                {
                    field: rule.field,
                    values: rule.values,
                    condition: BackendFilterOperator.Contain,
                    negated: true,
                },
            ];
        case 'within':
            return [
                {
                    field: rule.field,
                    values: rule.values,
                    condition: BackendFilterOperator.DescendantsIncl,
                    negated: false,
                },
            ];
        case 'is_not':
            return [
                {
                    field: rule.field,
                    values: rule.values,
                    condition: BackendFilterOperator.Equal,
                    negated: true,
                },
            ];
        case 'is_all':
            return [
                {
                    field: rule.field,
                    values: rule.values,
                    // AllEqual is frontend-only; generateOrFilters splits into AND Equals.
                    condition: FrontendFilterOperator.AllEqual as unknown as BackendFilterOperator,
                    negated: false,
                },
            ];
        case 'is':
        default:
            return [
                {
                    field: rule.field,
                    values: rule.values,
                    condition: BackendFilterOperator.Equal,
                    negated: false,
                },
            ];
    }
}

export function filterGroupToActiveFilters(
    group: FilterGroup,
    availableFilters: FacetMetadata[],
): { filters: FacetFilterInput[]; unionType: UnionType; draftRules: FilterRule[] } {
    const draftRules: FilterRule[] = [];
    const filters: FacetFilterInput[] = [];

    group.filters.forEach((rule) => {
        const searchField = getSearchFilterField(rule.field, availableFilters);
        const operator = getOperatorsForField(searchField).find((candidate) => candidate.value === rule.operator);
        const requiresValue = operator?.requiresValue !== false;

        if (requiresValue && !rule.values.length) {
            draftRules.push(rule);
            return;
        }

        // `between` with only one bound is still incomplete.
        if (rule.operator === 'between' && rule.values.length < 2) {
            draftRules.push(rule);
            return;
        }

        filters.push(...ruleToFacetFilters(rule));
    });

    const appliedFields = new Set(filters.map((filter) => filter.field));
    const prunedDrafts = draftRules.filter((rule) => !(isEmptyDefaultDraft(rule) && appliedFields.has(rule.field)));

    return {
        filters,
        unionType: group.match === 'any' ? UnionType.OR : UnionType.AND,
        draftRules: prunedDrafts,
    };
}

export function createDefaultDraftRules(
    fields: FilterField[],
    dismissedFields: Set<string> = new Set(),
    limit = 3,
): FilterRule[] {
    const availableByName = new Map(fields.map((field) => [field.field, field]));

    return DEFAULT_SEARCH_FILTER_FIELDS.filter((fieldName) => availableByName.has(fieldName))
        .filter((fieldName) => !dismissedFields.has(fieldName))
        .slice(0, limit)
        .flatMap((fieldName) => {
            const field = availableByName.get(fieldName);
            if (!field) return [];
            return [
                {
                    id: `default-${fieldName}`,
                    field: fieldName,
                    operator: field.defaultOperator,
                    values: [] as string[],
                },
            ];
        });
}
