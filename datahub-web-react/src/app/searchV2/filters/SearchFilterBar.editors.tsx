import { Checkbox, DatePicker, Input, Loader } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React, { useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import {
    ExpandToggle,
    ExpandToggleSpacer,
    FilterPopover,
    MenuState,
    NestedOptionIndent,
    OptionCheckboxSlot,
    OptionContent,
    OptionCount,
    OptionDescription,
    OptionLabel,
    OptionList,
    OptionRow,
} from '@components/components/FilterBar/components';
import { FilterValueEditorProps, FilterValueOption } from '@components/components/FilterBar/types';

import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import { TagColor } from '@app/searchV2/filters/FilterOption';
import { flattenFilterBarOptions, nestFilterBarOptions } from '@app/searchV2/filters/SearchFilterBar.nesting';
import {
    EntityFilterField,
    FieldType,
    FilterField as SearchFilterField,
    FilterValueOption as SearchFilterValueOption,
} from '@app/searchV2/filters/types';
import { getFilterIconAndLabel } from '@app/searchV2/filters/utils';
import {
    deduplicateOptions,
    useFilterOptionsBySearchQuery,
    useLoadAggregationOptions,
    useLoadSearchOptions,
} from '@app/searchV2/filters/value/utils';
import {
    DOMAINS_FILTER_NAME,
    FIELD_TAGS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';
import dayjs from '@utils/dayjs';

import { AggregationMetadata, Domain, Entity, EntityType, Tag } from '@types';

const DEBOUNCE_MS = 300;

const DateFieldLabel = styled.div`
    padding: 4px 8px;
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    font-weight: 600;
`;

const DateFieldBlock = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding: 4px 0;
`;

/** Same entity chrome as master's FilterEntityIcon (tag dots, domain badge, …). */
function getEntityOptionIcon(
    fieldName: string,
    entity: Entity | null | undefined,
    fallback: React.ReactNode,
): React.ReactNode {
    if ((fieldName === TAGS_FILTER_NAME || fieldName === FIELD_TAGS_FILTER_NAME) && entity?.type === EntityType.Tag) {
        return <TagColor color={(entity as Tag).properties?.colorHex || ''} colorHash={entity.urn} />;
    }
    if (fieldName === DOMAINS_FILTER_NAME && entity?.type === EntityType.Domain) {
        return <DomainColoredIcon domain={entity as Domain} size={20} fontSize={12} />;
    }
    if (fieldName === PLATFORM_FILTER_NAME && entity) {
        return fallback;
    }
    return fallback;
}

/**
 * Same icon path as master's FilterOption: getFilterIconAndLabel + tag/domain chrome.
 * Live aggregations only ship field.icon (generic), which is why dots were missing.
 */
function toBarOption(
    option: SearchFilterValueOption,
    fieldName: string,
    entityRegistry: ReturnType<typeof useEntityRegistry>,
): FilterValueOption {
    const { label, icon } = getFilterIconAndLabel(
        fieldName,
        option.value,
        entityRegistry,
        option.entity || null,
        14,
        option.displayName,
    );
    return {
        value: option.value,
        label: label || option.displayName || option.value,
        count: option.count,
        icon: getEntityOptionIcon(fieldName, option.entity, icon),
    };
}

function collectDescendantValues(option: FilterValueOption): string[] {
    return (option.children ?? []).flatMap((child) =>
        child.disabled ? collectDescendantValues(child) : [child.value, ...collectDescendantValues(child)],
    );
}

function hasNestedOptions(options: FilterValueOption[]): boolean {
    return options.some((option) => !!option.children?.length);
}

function optionMatchesQuery(option: FilterValueOption, normalizedQuery: string): boolean {
    if (
        option.label.toLocaleLowerCase().includes(normalizedQuery) ||
        option.description?.toLocaleLowerCase().includes(normalizedQuery)
    ) {
        return true;
    }
    return (option.children ?? []).some((child) => optionMatchesQuery(child, normalizedQuery));
}

function filterNestedOptionsByQuery(options: FilterValueOption[], query: string): FilterValueOption[] {
    if (!query) return options;
    const normalizedQuery = query.toLocaleLowerCase();
    return options
        .map((option) => {
            if (!optionMatchesQuery(option, normalizedQuery)) return null;
            if (!option.children?.length) return option;
            const children = filterNestedOptionsByQuery(option.children, query);
            if (
                option.label.toLocaleLowerCase().includes(normalizedQuery) ||
                option.description?.toLocaleLowerCase().includes(normalizedQuery)
            ) {
                return { ...option, children: option.children };
            }
            return { ...option, children };
        })
        .filter((option): option is FilterValueOption => option !== null);
}

function toAggregationMetadata(options: SearchFilterValueOption[]): AggregationMetadata[] {
    return options.map((option) => ({
        value: option.value,
        count: option.count ?? 0,
        entity: option.entity ?? undefined,
    }));
}

/**
 * Live aggregations (exclude-self) are the source of truth for membership once loaded —
 * otherwise selecting a tag collapses the menu to search facets on the filtered set.
 * Keep richer facet icons/labels when the same value appears in both.
 */
function mergeFacetAndLiveOptions(
    facetOptions: FilterValueOption[],
    liveOptions: FilterValueOption[],
): FilterValueOption[] {
    if (!liveOptions.length) return facetOptions;
    if (!facetOptions.length) return liveOptions;
    const facetByValue = new Map(facetOptions.map((option) => [option.value, option]));
    return liveOptions.map((live) => {
        const facet = facetByValue.get(live.value);
        if (!facet) return live;
        return {
            ...live,
            label: facet.label || live.label,
            icon: facet.icon ?? live.icon,
            count: live.count ?? facet.count,
            description: facet.description || live.description,
        };
    });
}

export function isEntitySearchField(field: SearchFilterField): field is EntityFilterField {
    return field.type === FieldType.ENTITY || !!(field as unknown as EntityFilterField).entityTypes?.length;
}

export function booleanFilterOptions(): FilterValueOption[] {
    return [
        { value: 'true', label: 'True' },
        { value: 'false', label: 'False' },
    ];
}

/** Entity / enum pickers — mirrors old EntityValueMenu + useSearchFilterDropdown:
 * search facets as the default list, live aggregations (excluding this field) to
 * refresh counts / restore values after a self-filter, autocomplete for typing.
 *
 * Do NOT pass field.entityTypes into aggregations — those mean "search Tag entities
 * when typing", not "aggregate the tags facet only across Tag entities" (empty).
 */
export function EntityFilterValueEditor({
    field,
    rule,
    onChange,
    trigger,
    searchField,
    compose,
}: FilterValueEditorProps & { searchField: SearchFilterField }) {
    const { t } = useTranslation('search');
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const isCompose = !!compose;
    const [isOpen, setIsOpen] = useState(false);
    const [query, setQuery] = useState('');
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const [draftValues, setDraftValues] = useState<string[]>(rule.values);
    const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set());

    useDebounce(() => setDebouncedQuery(query), DEBOUNCE_MS, [query]);

    const entityField = searchField as EntityFilterField;
    const canSearchEntities = isEntitySearchField(searchField) && !!entityField.entityTypes?.length;
    const panelVisible = isCompose || isOpen;
    const activeValues = isCompose ? rule.values : draftValues;

    const { options: aggregationOptions, loading: aggregationsLoading } = useLoadAggregationOptions({
        field: searchField,
        visible: panelVisible,
        includeCounts: true,
    });

    const { options: searchOptions, loading: searchLoading } = useLoadSearchOptions(
        entityField,
        debouncedQuery,
        !panelVisible || !canSearchEntities,
    );

    // Flat facet values — nesting is re-applied after merge so live + facet share one tree.
    const defaultOptions = useMemo(() => flattenFilterBarOptions(field.values ?? []), [field.values]);

    const localFilteredAggs = useFilterOptionsBySearchQuery(aggregationOptions, query);
    const mergedSearchOptions = useMemo(() => {
        if (!debouncedQuery) return [];
        return deduplicateOptions(localFilteredAggs, searchOptions);
    }, [debouncedQuery, localFilteredAggs, searchOptions]);

    // Keep the last non-empty live list while aggregations refetch after applying a value —
    // otherwise the menu collapses to the narrowed search facets (looks like tags "disappeared").
    const previousLiveOptionsRef = useRef<FilterValueOption[]>([]);

    const options = useMemo(() => {
        const liveSource = query ? [...localFilteredAggs, ...mergedSearchOptions] : aggregationOptions;
        const liveOptions = liveSource.map((option) => toBarOption(option, field.field, entityRegistry));
        if (liveOptions.length) {
            previousLiveOptionsRef.current = liveOptions;
        }
        let stableLiveOptions = liveOptions;
        if (!liveOptions.length && aggregationsLoading) {
            stableLiveOptions = previousLiveOptionsRef.current;
        }

        let withDefaults = defaultOptions;
        if (!defaultOptions.length && stableLiveOptions.length) {
            withDefaults = stableLiveOptions;
        } else if (!defaultOptions.length) {
            withDefaults = searchOptions.map((option) => toBarOption(option, field.field, entityRegistry));
        }
        const flatMerged = mergeFacetAndLiveOptions(withDefaults, stableLiveOptions);
        const aggregationsForNesting = toAggregationMetadata([
            ...aggregationOptions,
            ...mergedSearchOptions,
            ...searchOptions,
        ]);
        const nested = nestFilterBarOptions(field.field, flatMerged, aggregationsForNesting, entityRegistry);
        return filterNestedOptionsByQuery(nested, query);
    }, [
        aggregationOptions,
        aggregationsLoading,
        defaultOptions,
        entityRegistry,
        field.field,
        localFilteredAggs,
        mergedSearchOptions,
        query,
        searchOptions,
    ]);

    const flatSelectable = useMemo(
        () => flattenFilterBarOptions(options).filter((option) => !option.disabled),
        [options],
    );
    const showNestingColumn = useMemo(() => hasNestedOptions(options), [options]);
    const loading = aggregationsLoading || searchLoading;
    const areAllSelected =
        flatSelectable.length > 0 && flatSelectable.every((option) => activeValues.includes(option.value));

    const applyValues = (nextValues: string[]) => {
        if (isCompose) {
            onChange({ ...rule, values: nextValues });
            return;
        }
        setDraftValues(nextValues);
    };

    const commitAndClose = () => {
        const valuesChanged =
            draftValues.length !== rule.values.length ||
            draftValues.some((value) => !rule.values.includes(value)) ||
            rule.values.some((value) => !draftValues.includes(value));
        if (valuesChanged) onChange({ ...rule, values: draftValues });
        setIsOpen(false);
        setQuery('');
    };

    const open = () => {
        setDraftValues(rule.values);
        setIsOpen(true);
    };

    const toggleExpanded = (value: string) => {
        setCollapsed((current) => {
            const next = new Set(current);
            if (next.has(value)) next.delete(value);
            else next.add(value);
            return next;
        });
    };

    const toggleValue = (option: FilterValueOption) => {
        if (option.disabled) return;
        if (field.selectionMode === 'single') {
            const nextRule = { ...rule, values: [option.value] };
            if (isCompose) {
                onChange(nextRule);
                compose.onCommit(nextRule);
                return;
            }
            onChange(nextRule);
            setDraftValues([option.value]);
            setIsOpen(false);
            setQuery('');
            return;
        }
        const descendantValues = collectDescendantValues(option);
        const isSelected = activeValues.includes(option.value);
        applyValues(
            isSelected
                ? activeValues.filter((value) => value !== option.value && !descendantValues.includes(value))
                : [...activeValues.filter((value) => !descendantValues.includes(value)), option.value],
        );
    };

    const toggleAllVisible = () =>
        applyValues(
            areAllSelected
                ? activeValues.filter((value) => !flatSelectable.some((option) => option.value === value))
                : Array.from(new Set([...activeValues, ...flatSelectable.map((option) => option.value)])),
        );

    const renderOption = (option: FilterValueOption, depth: number): React.ReactNode => {
        const hasChildren = !!option.children?.length;
        const isExpanded = hasChildren && !collapsed.has(option.value);
        const childValues = collectDescendantValues(option);
        const isChecked = activeValues.includes(option.value);
        const isIntermediate =
            !isChecked && childValues.some((value) => activeValues.includes(value)) && !option.disabled;

        return (
            <React.Fragment key={option.value}>
                <OptionRow
                    type="button"
                    disabled={option.disabled && !hasChildren}
                    onClick={() => {
                        if (option.disabled && hasChildren) {
                            toggleExpanded(option.value);
                            return;
                        }
                        toggleValue(option);
                    }}
                >
                    {showNestingColumn && depth > 0 && <NestedOptionIndent $depth={depth} />}
                    {showNestingColumn &&
                        (hasChildren ? (
                            <ExpandToggle
                                type="button"
                                aria-label={isExpanded ? tc('collapse') : tc('expand')}
                                onClick={(event) => {
                                    event.stopPropagation();
                                    toggleExpanded(option.value);
                                }}
                            >
                                {isExpanded ? (
                                    <CaretDown size={14} weight="bold" />
                                ) : (
                                    <CaretRight size={14} weight="bold" />
                                )}
                            </ExpandToggle>
                        ) : (
                            <ExpandToggleSpacer />
                        ))}
                    {option.icon}
                    <OptionContent>
                        <OptionLabel>{option.label}</OptionLabel>
                        {option.description && <OptionDescription>{option.description}</OptionDescription>}
                    </OptionContent>
                    {option.count !== undefined && <OptionCount>{option.count.toLocaleString()}</OptionCount>}
                    <OptionCheckboxSlot>
                        {field.selectionMode !== 'single' && !option.disabled ? (
                            <Checkbox
                                isChecked={isChecked}
                                isIntermediate={isIntermediate}
                                onCheckboxChange={() => toggleValue(option)}
                                size="sm"
                            />
                        ) : null}
                    </OptionCheckboxSlot>
                </OptionRow>
                {isExpanded && option.children?.map((child) => renderOption(child, depth + 1))}
            </React.Fragment>
        );
    };

    const panel = (
        <>
            {field.searchable !== false && (
                <Input
                    value={query}
                    setValue={setQuery}
                    placeholder={t('filters.searchValues')}
                    icon={{ icon: MagnifyingGlass }}
                    onClear={() => setQuery('')}
                />
            )}
            <OptionList>
                {field.showSelectAll !== false && field.selectionMode !== 'single' && !!flatSelectable.length && (
                    <OptionRow type="button" onClick={toggleAllVisible}>
                        <OptionContent>{t('filters.selectAll')}</OptionContent>
                        <OptionCheckboxSlot>
                            <Checkbox
                                isChecked={areAllSelected}
                                isIntermediate={
                                    !areAllSelected &&
                                    flatSelectable.some((option) => activeValues.includes(option.value))
                                }
                                onCheckboxChange={toggleAllVisible}
                                size="sm"
                            />
                        </OptionCheckboxSlot>
                    </OptionRow>
                )}
                {options.map((option) => renderOption(option, 0))}
                {loading && (
                    <MenuState>
                        <Loader size="sm" />
                    </MenuState>
                )}
                {!loading && !options.length && <MenuState>{t('filters.noValuesFound')}</MenuState>}
            </OptionList>
        </>
    );

    if (isCompose) return panel;

    return (
        <FilterPopover
            isOpen={isOpen}
            onClose={commitAndClose}
            trigger={React.cloneElement(trigger, { onClick: () => (isOpen ? commitAndClose() : open()) })}
        >
            {panel}
        </FilterPopover>
    );
}

/** Custom date range → stores [startMs, endMs] with operator `between`. */
export function DateRangeFilterValueEditor({ rule, onChange, trigger, compose }: FilterValueEditorProps) {
    const { t } = useTranslation('search');
    const isCompose = !!compose;
    const [isOpen, setIsOpen] = useState(false);
    const startMs = rule.values[0] ? Number(rule.values[0]) : undefined;
    const endMs = rule.values[1] ? Number(rule.values[1]) : undefined;

    const updateRange = (nextStart: string, nextEnd: string) => {
        let nextValues: string[] = [];
        if (nextStart && nextEnd) {
            nextValues = [nextStart, nextEnd];
        } else if (nextStart) {
            nextValues = [nextStart];
        } else if (nextEnd) {
            nextValues = [nextEnd];
        }
        onChange({
            ...rule,
            operator: 'between',
            values: nextValues,
        });
    };

    const panel = (
        <>
            <DateFieldBlock>
                <DateFieldLabel>{t('filters.dateRange.start')}</DateFieldLabel>
                <DatePicker
                    value={startMs ? dayjs(startMs) : undefined}
                    onChange={(value) => {
                        const nextStart = value ? value.startOf('day').valueOf().toString() : '';
                        updateRange(nextStart, rule.values[1] || '');
                    }}
                />
            </DateFieldBlock>
            <DateFieldBlock>
                <DateFieldLabel>{t('filters.dateRange.end')}</DateFieldLabel>
                <DatePicker
                    value={endMs ? dayjs(endMs) : undefined}
                    onChange={(value) => {
                        const nextEnd = value ? value.endOf('day').valueOf().toString() : '';
                        updateRange(rule.values[0] || '', nextEnd);
                    }}
                />
            </DateFieldBlock>
        </>
    );

    if (isCompose) return panel;

    return (
        <FilterPopover
            isOpen={isOpen}
            onClose={() => setIsOpen(false)}
            trigger={React.cloneElement(trigger, { onClick: () => setIsOpen((open) => !open) })}
            width={320}
        >
            {panel}
        </FilterPopover>
    );
}
