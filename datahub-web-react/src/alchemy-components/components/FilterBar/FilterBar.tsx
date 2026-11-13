import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { RefObject, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';

import { Checkbox } from '@components/components/Checkbox';
import {
    AddFilterMenu,
    Chip,
    ChipSegment,
    Container,
    ExpandToggle,
    ExpandToggleSpacer,
    FLYOUT_ANCHOR_ATTRIBUTE,
    FieldName,
    FilterPopover,
    FiltersRow,
    GhostTrigger,
    GroupActions,
    GroupContainer,
    GroupRemoveButton,
    MatchTabs,
    MenuAnchorContext,
    MenuOpenContext,
    MenuPlacementContext,
    MenuState,
    NestedOptionIndent,
    OperatorText,
    OptionCheckboxSlot,
    OptionContent,
    OptionCount,
    OptionCountLoader,
    OptionDescription,
    OptionLabel,
    OptionList,
    OptionRow,
    RemoveButton,
    ValueContent,
    ValueEditorPanel,
    ValueFlyoutPanel,
    ValueIconStack,
    ValueIconStackItem,
    ValueList,
    ValueListIcon,
    ValueListItem,
    ValueOverflowCount,
    ValueText,
} from '@components/components/FilterBar/components';
import {
    FilterBarLabels,
    FilterBarProps,
    FilterField,
    FilterGroup,
    FilterMatchMode,
    FilterOperator,
    FilterRule,
    FilterValueOption,
} from '@components/components/FilterBar/types';
import { Input } from '@components/components/Input';
import { Loader } from '@components/components/Loader';
import { Tooltip } from '@components/components/Tooltip';

function groupAnchorKey(group: string): string {
    return `group:${group}`;
}

const DEFAULT_LABELS: FilterBarLabels = {
    addFilter: 'Filter',
    addGroup: 'Add group',
    all: 'all',
    any: 'any',
    back: 'Back',
    chooseValue: 'Choose value',
    clearAll: 'Clear all',
    collapse: 'Collapse',
    expand: 'Expand',
    noFilters: 'No filters found',
    removeFilter: 'Remove filter',
    removeGroup: 'Remove group',
    searchFilters: 'Search filters',
    searchValues: 'Search values',
    selectAll: 'Select all',
    where: 'Where',
};

/** Distance from the bottom of the value list at which the next page is requested. */
const LOAD_MORE_THRESHOLD_PX = 48;

function createId(prefix: string): string {
    return `${prefix}-${globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random()}`}`;
}

function getOperator(field: FilterField, value: string): FilterOperator | undefined {
    return field.operators.find((operator) => operator.value === value) ?? field.operators?.[0];
}

function getSelectedOptions(field: FilterField, values: string[]): FilterValueOption[] {
    const availableOptions = [...(field.values ?? []), ...(field.selectedOptions ?? [])];
    const flattened = flattenFilterOptions(availableOptions);
    return values.map((value) => flattened.find((option) => option.value === value) ?? { value, label: value });
}

const MAX_NAMED_VALUES = 2;

function getValueLabel(selectedOptions: FilterValueOption[], placeholder: string): string {
    if (!selectedOptions.length) return placeholder;
    return selectedOptions
        .slice(0, MAX_NAMED_VALUES)
        .map((option) => option.label)
        .join(', ');
}

/** Whether the element's text is cut off, rechecked as its width changes. */
function useIsTruncated(ref: RefObject<HTMLElement>, text: string): boolean {
    const [isTruncated, setIsTruncated] = useState(false);
    useLayoutEffect(() => {
        const el = ref.current;
        if (!el) return undefined;
        const update = () => setIsTruncated(el.scrollWidth > el.clientWidth);
        update();
        if (typeof ResizeObserver === 'undefined') return undefined;
        const resizeObserver = new ResizeObserver(update);
        resizeObserver.observe(el);
        return () => resizeObserver.disconnect();
    }, [ref, text]);
    return isTruncated;
}

function flattenFilterOptions(options: FilterValueOption[]): FilterValueOption[] {
    return options.flatMap((option) => [option, ...flattenFilterOptions(option.children ?? [])]);
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

function filterOptionsByQuery(options: FilterValueOption[], query: string): FilterValueOption[] {
    if (!query) return options;
    const normalizedQuery = query.toLocaleLowerCase();
    return options
        .map((option) => {
            if (!optionMatchesQuery(option, normalizedQuery)) return null;
            if (!option.children?.length) return option;
            const children = filterOptionsByQuery(option.children, query);
            // Keep parent when it matches or when any child matches.
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

function collectDescendantValues(option: FilterValueOption): string[] {
    return (option.children ?? []).flatMap((child) =>
        child.disabled ? collectDescendantValues(child) : [child.value, ...collectDescendantValues(child)],
    );
}

function hasNestedOptions(options: FilterValueOption[]): boolean {
    return options.some((option) => !!option.children?.length);
}

type ValueEditorProps = {
    rule: FilterRule;
    field: FilterField;
    labels: FilterBarLabels;
    trigger: React.ReactElement;
    onChange: (rule: FilterRule) => void;
    compose?: {
        onCommit: (rule: FilterRule) => void;
    };
};

function haveSameValues(a: string[], b: string[]): boolean {
    return a.length === b.length && a.every((value) => b.includes(value)) && b.every((value) => a.includes(value));
}

function BuiltInValueEditor({ rule, field, labels, trigger, onChange, compose }: ValueEditorProps) {
    const isCompose = !!compose;
    const [isOpen, setIsOpen] = useState(false);
    const [query, setQuery] = useState('');
    const [draftValues, setDraftValues] = useState<string[]>(rule.values);
    const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set());
    // Picked while searching server-side, so a later search or clearing it does not drop them from the list
    const [pickedOptions, setPickedOptions] = useState<FilterValueOption[]>([]);

    const activeValues = isCompose ? rule.values : draftValues;

    const options = useMemo(() => {
        const listed = flattenFilterOptions(field.values ?? []);
        const extra: FilterValueOption[] = [];
        [...(field.selectedOptions ?? []), ...pickedOptions].forEach((option) => {
            const isListed = [...listed, ...extra].some((candidate) => candidate.value === option.value);
            if (!isListed) extra.push(option);
        });
        const fieldValues = [...(field.values ?? []), ...extra];
        if (field.onSearch || !query) return fieldValues;
        return filterOptionsByQuery(fieldValues, query);
    }, [field.onSearch, field.selectedOptions, field.values, pickedOptions, query]);

    const onSearchRef = useRef(field.onSearch);
    onSearchRef.current = field.onSearch;
    const queryRef = useRef(query);
    queryRef.current = query;
    useEffect(
        () => () => {
            if (queryRef.current) onSearchRef.current?.('');
        },
        [],
    );

    const clearQuery = () => {
        if (query) field.onSearch?.('');
        setQuery('');
    };

    // The bar may be removed with the menu still open, e.g. with the panel holding it, which must not drop the draft
    const pendingDraftRef = useRef({ isOpen, draftValues, rule, onChange });
    pendingDraftRef.current = { isOpen, draftValues, rule, onChange };
    useEffect(
        () => () => {
            const pending = pendingDraftRef.current;
            if (pending.isOpen && !haveSameValues(pending.draftValues, pending.rule.values)) {
                pending.onChange({ ...pending.rule, values: pending.draftValues });
            }
        },
        [],
    );

    const flatSelectable = useMemo(() => flattenFilterOptions(options).filter((option) => !option.disabled), [options]);
    const showNestingColumn = useMemo(() => hasNestedOptions(options), [options]);
    const areAllVisibleSelected =
        flatSelectable.length > 0 && flatSelectable.every((option) => activeValues.includes(option.value));

    const applyValues = (nextValues: string[]) => {
        if (isCompose) {
            onChange({ ...rule, values: nextValues });
            return;
        }
        setDraftValues(nextValues);
    };

    const commitAndClose = () => {
        if (!haveSameValues(draftValues, rule.values)) {
            onChange({ ...rule, values: draftValues });
        }
        setIsOpen(false);
        clearQuery();
    };

    const open = () => {
        setDraftValues(rule.values);
        setIsOpen(true);
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
            clearQuery();
            return;
        }

        const descendantValues = collectDescendantValues(option);
        const isSelected = activeValues.includes(option.value);
        if (!isSelected && field.onSearch) setPickedOptions((current) => [...current, option]);
        const nextValues = isSelected
            ? activeValues.filter((value) => value !== option.value && !descendantValues.includes(value))
            : [...activeValues.filter((value) => !descendantValues.includes(value)), option.value];
        applyValues(nextValues);
    };

    const toggleAllVisible = () =>
        applyValues(
            areAllVisibleSelected
                ? activeValues.filter((value) => !flatSelectable.some((option) => option.value === value))
                : Array.from(new Set([...activeValues, ...flatSelectable.map((option) => option.value)])),
        );

    const toggleExpanded = (value: string) => {
        setCollapsed((current) => {
            const next = new Set(current);
            if (next.has(value)) next.delete(value);
            else next.add(value);
            return next;
        });
    };

    const onScroll = (event: React.UIEvent<HTMLDivElement>) => {
        if (!field.hasMore || field.loading) return;
        const { scrollHeight, scrollTop, clientHeight } = event.currentTarget;
        if (scrollHeight - scrollTop - clientHeight < LOAD_MORE_THRESHOLD_PX) field.onLoadMore?.();
    };

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
                    data-testid={`filter-bar-value-option-${option.value}`}
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
                                aria-label={isExpanded ? labels.collapse : labels.expand}
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
                    {!field.renderValueOption && option.icon}
                    {field.renderValueOption ? (
                        field.renderValueOption(option)
                    ) : (
                        <OptionContent>
                            <OptionLabel>{option.label}</OptionLabel>
                            {option.description && <OptionDescription>{option.description}</OptionDescription>}
                        </OptionContent>
                    )}
                    {option.count !== undefined && (
                        <OptionCount data-testid="filter-bar-option-count">{option.count.toLocaleString()}</OptionCount>
                    )}
                    {option.count === undefined && field.countsLoading && (
                        <OptionCountLoader data-testid="filter-bar-option-count-loading">
                            <Loader size="xs" />
                        </OptionCountLoader>
                    )}
                    <OptionCheckboxSlot>
                        {field.selectionMode !== 'single' && !option.disabled && (
                            <Checkbox
                                isChecked={isChecked}
                                isIntermediate={isIntermediate}
                                onCheckboxChange={() => toggleValue(option)}
                                size="sm"
                            />
                        )}
                        {field.selectionMode === 'single' && isChecked && <Check size={16} />}
                    </OptionCheckboxSlot>
                </OptionRow>
                {isExpanded && option.children?.map((child) => renderOption(child, depth + 1))}
            </React.Fragment>
        );
    };

    const panel = (
        <>
            {field.searchable && (
                <Input
                    value={query}
                    setValue={(value) => {
                        setQuery(value);
                        field.onSearch?.(value);
                    }}
                    placeholder={labels.searchValues}
                    inputTestId="filter-bar-value-search"
                    icon={{ icon: MagnifyingGlass }}
                    onClear={clearQuery}
                />
            )}
            <OptionList onScroll={onScroll}>
                {field.showSelectAll && field.selectionMode !== 'single' && !!flatSelectable.length && (
                    <OptionRow type="button" onClick={toggleAllVisible}>
                        <OptionContent>{labels.selectAll}</OptionContent>
                        <OptionCheckboxSlot>
                            <Checkbox
                                isChecked={areAllVisibleSelected}
                                isIntermediate={
                                    !areAllVisibleSelected &&
                                    flatSelectable.some((option) => activeValues.includes(option.value))
                                }
                                onCheckboxChange={toggleAllVisible}
                                size="sm"
                            />
                        </OptionCheckboxSlot>
                    </OptionRow>
                )}
                {options.map((option) => renderOption(option, 0))}
                {field.loading && (
                    <MenuState>
                        <Loader size="sm" />
                    </MenuState>
                )}
                {!field.loading && !options.length && <MenuState>{labels.noFilters}</MenuState>}
            </OptionList>
        </>
    );

    if (isCompose) return <ValueEditorPanel data-testid="filter-bar-value-menu">{panel}</ValueEditorPanel>;

    return (
        <FilterPopover
            isOpen={isOpen}
            onClose={commitAndClose}
            trigger={React.cloneElement(trigger, { onClick: () => (isOpen ? commitAndClose() : open()) })}
            shrinkable
        >
            <ValueEditorPanel data-testid="filter-bar-value-menu">{panel}</ValueEditorPanel>
        </FilterPopover>
    );
}

type OperatorEditorProps = {
    rule: FilterRule;
    field: FilterField;
    onChange: (rule: FilterRule) => void;
};

function OperatorEditor({ rule, field, onChange }: OperatorEditorProps) {
    const [isOpen, setIsOpen] = useState(false);
    const operator = getOperator(field, rule.operator);
    const displayLabel = rule.values.length > 1 && operator?.pluralLabel ? operator.pluralLabel : operator?.label;
    const visibleOperators = field.operators.filter(
        (option) => rule.values.length >= (option.minValues ?? 0) || option.value === rule.operator,
    );

    return (
        <FilterPopover
            isOpen={isOpen}
            onClose={() => setIsOpen(false)}
            width={220}
            trigger={
                <ChipSegment type="button" data-testid="filter-bar-operator" onClick={() => setIsOpen(!isOpen)}>
                    {displayLabel}
                </ChipSegment>
            }
        >
            <OptionList>
                {visibleOperators.map((option) => {
                    const optionLabel =
                        rule.values.length > 1 && option.pluralLabel ? option.pluralLabel : option.label;
                    return (
                        <OptionRow
                            key={option.value}
                            type="button"
                            data-testid={`filter-bar-operator-${option.value}`}
                            onClick={() => {
                                onChange({
                                    ...rule,
                                    operator: option.value,
                                    values: option.requiresValue === false ? [] : rule.values,
                                });
                                setIsOpen(false);
                            }}
                        >
                            <OptionContent>{optionLabel}</OptionContent>
                            {option.value === rule.operator && <Check size={16} />}
                        </OptionRow>
                    );
                })}
            </OptionList>
        </FilterPopover>
    );
}

type AddFilterPickerProps = {
    fields: FilterField[];
    labels: FilterBarLabels;
    /**
     * Linear-style: apply values as soon as the user toggles them.
     * Pass `ruleId` to update an in-progress chip; omit to create one.
     * Returns the chip id while values remain, or undefined when removed / empty.
     */
    onUpsert: (input: {
        field: FilterField;
        values: string[];
        operator?: string;
        ruleId?: string | null;
    }) => string | undefined;
};

function fieldMatchesQuery(field: FilterField, normalizedQuery: string): boolean {
    return (
        field.label.toLocaleLowerCase().includes(normalizedQuery) ||
        !!field.description?.toLocaleLowerCase().includes(normalizedQuery) ||
        !!field.group?.toLocaleLowerCase().includes(normalizedQuery)
    );
}

function AddFilterPicker({ fields, labels, onUpsert }: AddFilterPickerProps) {
    const [isOpen, setIsOpen] = useState(false);
    const [query, setQuery] = useState('');
    const [activeGroup, setActiveGroup] = useState<string | null>(null);
    const [pendingField, setPendingField] = useState<FilterField | null>(null);
    const [composeRule, setComposeRule] = useState<FilterRule | null>(null);
    /** Chip created for the current hover-compose session (updated on each toggle). */
    const [committedRuleId, setCommittedRuleId] = useState<string | null>(null);
    const menuRef = useRef<HTMLDivElement>(null);
    // Set by Enter / ArrowRight on a row, so the keyboard can reach the flyout that row opened
    const [flyoutFocusRequest, setFlyoutFocusRequest] = useState<'values' | 'group' | null>(null);
    const committedRuleIdRef = useRef<string | null>(null);
    committedRuleIdRef.current = committedRuleId;

    const { rootFields, groups } = useMemo(() => {
        const nextRoot: FilterField[] = [];
        const nextGroups = new Map<string, FilterField[]>();

        // Preserve caller order (SORTED_FILTERS priority) — do not alphabetize.
        // A field hidden by the rule it is composing stays listed until another field is picked.
        const listed = fields.filter((field) => !field.hidden || field.field === pendingField?.field);
        listed.forEach((field) => {
            if (!field.group) {
                nextRoot.push(field);
                return;
            }
            nextGroups.set(field.group, [...(nextGroups.get(field.group) ?? []), field]);
        });

        return { rootFields: nextRoot, groups: nextGroups };
    }, [fields, pendingField?.field]);

    const normalizedQuery = query.trim().toLocaleLowerCase();
    const isSearching = !!normalizedQuery;

    const visibleRootFields = isSearching
        ? rootFields.filter((field) => fieldMatchesQuery(field, normalizedQuery))
        : rootFields;

    const visibleGroups = useMemo(() => {
        const entries = Array.from(groups.entries()).sort(([left], [right]) =>
            left.localeCompare(right, undefined, { sensitivity: 'base' }),
        );
        if (!isSearching) return entries;
        return entries
            .map(
                ([group, groupFields]) =>
                    [group, groupFields.filter((field) => fieldMatchesQuery(field, normalizedQuery))] as const,
            )
            .filter(([, groupFields]) => groupFields.length > 0);
    }, [groups, isSearching, normalizedQuery]);

    const activeGroupFields = activeGroup
        ? (groups.get(activeGroup) ?? []).filter((field) => !isSearching || fieldMatchesQuery(field, normalizedQuery))
        : [];

    const resetCompose = () => {
        setPendingField(null);
        setComposeRule(null);
        committedRuleIdRef.current = null;
        setCommittedRuleId(null);
    };

    const close = () => {
        // Values already applied on toggle — closing never drops committed chips.
        setIsOpen(false);
        setQuery('');
        setActiveGroup(null);
        resetCompose();
    };

    /** Push compose values into the filter bar immediately (Linear-style). */
    const syncComposeToBar = (field: FilterField, rule: FilterRule) => {
        const activeRuleId = committedRuleIdRef.current;
        if (!rule.values.length) {
            if (activeRuleId) {
                onUpsert({ field, values: [], ruleId: activeRuleId });
                committedRuleIdRef.current = null;
                setCommittedRuleId(null);
            }
            return;
        }
        if (rule.values.length < (getOperator(field, rule.operator)?.minValues ?? 0) && !activeRuleId) {
            return;
        }
        const nextId = onUpsert({
            field,
            values: rule.values,
            operator: rule.operator,
            ruleId: activeRuleId,
        });
        committedRuleIdRef.current = nextId ?? null;
        setCommittedRuleId(nextId ?? null);
    };

    /** Open the value flyout for a field (hover or click). Does not clear other chips. */
    const previewCompose = (field: FilterField) => {
        if (pendingField?.field === field.field) return;
        setPendingField(field);
        committedRuleIdRef.current = null;
        setCommittedRuleId(null);
        if (getOperator(field, field.defaultOperator)?.requiresValue === false) {
            setComposeRule(null);
            return;
        }
        setComposeRule({
            id: 'compose',
            field: field.field,
            operator: field.defaultOperator,
            values: [],
        });
    };

    const onComposeRuleChange = (rule: FilterRule) => {
        setComposeRule(rule);
        if (!pendingField) return;
        syncComposeToBar(pendingField, rule);
    };

    const commitCompose = (rule: FilterRule) => {
        if (!pendingField) return;
        setComposeRule(rule);
        syncComposeToBar(pendingField, rule);
    };

    const onFieldActivate = (field: FilterField) => {
        const operator = getOperator(field, field.defaultOperator);
        if (operator?.requiresValue === false) {
            onUpsert({ field, values: [], operator: field.defaultOperator });
            close();
            return;
        }
        previewCompose(field);
    };

    const previewGroup = (group: string) => {
        setActiveGroup(group);
        resetCompose();
    };

    const previewRootField = (field: FilterField) => {
        setActiveGroup(null);
        previewCompose(field);
    };

    const composeField = pendingField
        ? (fields.find((candidate) => candidate.field === pendingField.field) ?? pendingField)
        : null;
    const composeInActiveGroup =
        !!activeGroup && !!composeField && activeGroupFields.some((field) => field.field === composeField.field);
    const showRootValueFlyout = !!composeField && !!composeRule && !activeGroup;
    const showGroupValueFlyout = !!composeField && !!composeRule && composeInActiveGroup;
    const showGroupFlyout = !!activeGroup && !isSearching;
    const hasResults = visibleRootFields.length > 0 || visibleGroups.length > 0;

    const composeTrigger = <span />;

    const renderValueCompose = (field: FilterField) =>
        field.renderValueEditor ? (
            field.renderValueEditor({
                field,
                rule: composeRule!,
                onChange: onComposeRuleChange,
                trigger: composeTrigger,
                compose: { onCommit: commitCompose },
            })
        ) : (
            <BuiltInValueEditor
                rule={composeRule!}
                field={field}
                labels={labels}
                trigger={composeTrigger}
                onChange={onComposeRuleChange}
                compose={{ onCommit: commitCompose }}
            />
        );

    useEffect(() => {
        if (!flyoutFocusRequest) return;
        const selector =
            flyoutFocusRequest === 'values'
                ? '[data-testid="filter-bar-value-menu"] input, [data-testid="filter-bar-value-menu"] button'
                : '[data-testid="filter-bar-group-flyout"] button';
        menuRef.current?.querySelector<HTMLElement>(selector)?.focus();
        setFlyoutFocusRequest(null);
    }, [flyoutFocusRequest, composeRule, activeGroup]);

    const onRowKeyDown = (event: React.KeyboardEvent, open: () => void, target: 'values' | 'group') => {
        if (event.key !== 'Enter' && event.key !== 'ArrowRight') return;
        event.preventDefault();
        open();
        setFlyoutFocusRequest(target);
    };

    const renderFieldRow = (
        field: FilterField,
        options?: { description?: string; onHover?: (field: FilterField) => void },
    ) => (
        <OptionRow
            key={field.field}
            type="button"
            data-testid={`filter-bar-field-${field.field}`}
            {...{ [FLYOUT_ANCHOR_ATTRIBUTE]: field.field }}
            $active={pendingField?.field === field.field}
            onMouseEnter={() => (options?.onHover ?? previewCompose)(field)}
            onFocus={() => (options?.onHover ?? previewCompose)(field)}
            onClick={() => onFieldActivate(field)}
            onKeyDown={(event) => onRowKeyDown(event, () => onFieldActivate(field), 'values')}
        >
            <OptionContent>
                <OptionLabel>{field.label}</OptionLabel>
                {(options?.description || field.description) && (
                    <OptionDescription>{options?.description || field.description}</OptionDescription>
                )}
            </OptionContent>
            {getOperator(field, field.defaultOperator)?.requiresValue !== false && <CaretRight size={14} />}
        </OptionRow>
    );

    // Kept open while the field being composed is the last one, which its new rule hides
    if (!isOpen && !fields.some((field) => !field.hidden)) return null;

    return (
        <FilterPopover
            isOpen={isOpen}
            onClose={close}
            trigger={
                <GhostTrigger
                    type="button"
                    data-testid="filter-bar-add"
                    onClick={() => (isOpen ? close() : setIsOpen(true))}
                >
                    <Plus size={14} />
                    {labels.addFilter}
                </GhostTrigger>
            }
        >
            <AddFilterMenu ref={menuRef}>
                <Input
                    value={query}
                    setValue={(value) => {
                        setQuery(value);
                        setActiveGroup(null);
                        resetCompose();
                    }}
                    placeholder={labels.searchFilters}
                    inputTestId="filter-bar-field-search"
                    icon={{ icon: MagnifyingGlass }}
                    onClear={() => {
                        setQuery('');
                        setActiveGroup(null);
                        resetCompose();
                    }}
                />
                <OptionList>
                    {visibleRootFields.map((field) => renderFieldRow(field, { onHover: previewRootField }))}
                    {isSearching
                        ? visibleGroups.flatMap(([group, groupFields]) =>
                              groupFields.map((field) =>
                                  renderFieldRow(field, {
                                      description: group,
                                      onHover: previewRootField,
                                  }),
                              ),
                          )
                        : visibleGroups.map(([group]) => (
                              <OptionRow
                                  key={group}
                                  type="button"
                                  {...{ [FLYOUT_ANCHOR_ATTRIBUTE]: groupAnchorKey(group) }}
                                  $active={activeGroup === group}
                                  onMouseEnter={() => previewGroup(group)}
                                  onFocus={() => previewGroup(group)}
                                  onClick={() => previewGroup(group)}
                                  onKeyDown={(event) => onRowKeyDown(event, () => previewGroup(group), 'group')}
                              >
                                  <OptionContent>
                                      <OptionLabel>{group}</OptionLabel>
                                  </OptionContent>
                                  <CaretRight size={14} />
                              </OptionRow>
                          ))}
                    {!hasResults && <MenuState>{labels.noFilters}</MenuState>}
                </OptionList>
                {showGroupFlyout && (
                    <ValueFlyoutPanel
                        aria-label={activeGroup ?? undefined}
                        anchorKey={groupAnchorKey(activeGroup)}
                        data-testid="filter-bar-group-flyout"
                    >
                        <OptionList>
                            {activeGroupFields.map((field) => renderFieldRow(field))}
                            {!activeGroupFields.length && <MenuState>{labels.noFilters}</MenuState>}
                        </OptionList>
                        {showGroupValueFlyout && composeField && (
                            <ValueFlyoutPanel aria-label={composeField.label} anchorKey={composeField.field}>
                                <React.Fragment key={composeField.field}>
                                    {renderValueCompose(composeField)}
                                </React.Fragment>
                            </ValueFlyoutPanel>
                        )}
                    </ValueFlyoutPanel>
                )}
                {showRootValueFlyout && composeField && (
                    <ValueFlyoutPanel aria-label={composeField.label} anchorKey={composeField.field}>
                        <React.Fragment key={composeField.field}>{renderValueCompose(composeField)}</React.Fragment>
                    </ValueFlyoutPanel>
                )}
            </AddFilterMenu>
        </FilterPopover>
    );
}

type RuleChipProps = {
    rule: FilterRule;
    field: FilterField;
    labels: FilterBarLabels;
    onChange: (rule: FilterRule) => void;
    onRemove: () => void;
};

function RuleChip({ rule, field, labels, onChange, onRemove }: RuleChipProps) {
    const operator = getOperator(field, rule.operator);
    const selectedOptions = getSelectedOptions(field, rule.values);
    const namedOptions = selectedOptions.slice(0, MAX_NAMED_VALUES);
    const stackedIcons = namedOptions.filter((option) => option.icon);
    const unnamedCount = selectedOptions.length - namedOptions.length;
    const valueLabel = getValueLabel(selectedOptions, labels.chooseValue);
    const valueTextRef = useRef<HTMLSpanElement>(null);
    const isTruncated = useIsTruncated(valueTextRef, valueLabel);
    // The label names at most two values, so list them all whenever any are cut off or left out
    const showsAllValues = !isTruncated && !unnamedCount;
    const valueTrigger = (
        <ChipSegment type="button" data-testid="filter-bar-value" $emphasized $placeholder={!rule.values.length}>
            <Tooltip
                title={
                    showsAllValues ? undefined : (
                        <ValueList data-testid="filter-bar-value-tooltip">
                            {selectedOptions.map((option) => (
                                <ValueListItem key={option.value}>
                                    {option.icon && <ValueListIcon>{option.icon}</ValueListIcon>}
                                    {option.label}
                                </ValueListItem>
                            ))}
                        </ValueList>
                    )
                }
                placement="top"
            >
                <ValueContent>
                    {!!stackedIcons.length && (
                        <ValueIconStack>
                            {stackedIcons.map((option) => (
                                <ValueIconStackItem key={option.value}>{option.icon}</ValueIconStackItem>
                            ))}
                        </ValueIconStack>
                    )}
                    <ValueText ref={valueTextRef}>{valueLabel}</ValueText>
                    {!!unnamedCount && (
                        <ValueOverflowCount data-testid="filter-bar-value-count">{`+${unnamedCount}`}</ValueOverflowCount>
                    )}
                </ValueContent>
            </Tooltip>
        </ChipSegment>
    );

    return (
        <Chip data-testid={`filter-bar-chip-${field.field}`}>
            <FieldName>{field.label}</FieldName>
            {field.operators.length > 1 ? (
                <OperatorEditor rule={rule} field={field} onChange={onChange} />
            ) : (
                !!operator?.label && <OperatorText>{operator.label}</OperatorText>
            )}
            {operator?.requiresValue !== false &&
                (field.renderValueEditor ? (
                    field.renderValueEditor({ field, rule, onChange, trigger: valueTrigger })
                ) : (
                    <BuiltInValueEditor
                        rule={rule}
                        field={field}
                        labels={labels}
                        trigger={valueTrigger}
                        onChange={onChange}
                    />
                ))}
            <RemoveButton type="button" aria-label={labels.removeFilter} onClick={onRemove}>
                <X size={14} />
            </RemoveButton>
        </Chip>
    );
}

type GroupProps = {
    group: FilterGroup;
    fields: FilterField[];
    labels: FilterBarLabels;
    depth: number;
    maxDepth: number;
    allowGroups: boolean;
    addFilterPlacement: 'inline' | 'below';
    onChange: (group: FilterGroup) => void;
    onRemove?: () => void;
};

function FilterGroupView({
    group,
    fields,
    labels,
    depth,
    maxDepth,
    allowGroups,
    addFilterPlacement,
    onChange,
    onRemove,
}: GroupProps) {
    // Same field can be added more than once (e.g. Owner is X and Owner is not Y).
    // Add Filter commits on each value toggle and updates the in-progress chip by id.
    const upsertFilter = ({
        field,
        values,
        operator,
        ruleId,
    }: {
        field: FilterField;
        values: string[];
        operator?: string;
        ruleId?: string | null;
    }): string | undefined => {
        const takesValues = getOperator(field, operator ?? field.defaultOperator)?.requiresValue !== false;
        if (!values.length && takesValues) {
            if (ruleId) {
                onChange({
                    ...group,
                    filters: group.filters.filter((filter) => filter.id !== ruleId),
                });
            }
            return undefined;
        }

        if (ruleId && group.filters.some((filter) => filter.id === ruleId)) {
            onChange({
                ...group,
                filters: group.filters.map((filter) =>
                    filter.id === ruleId ? { ...filter, values, operator: operator ?? filter.operator } : filter,
                ),
            });
            return ruleId;
        }

        const id = ruleId || createId('filter');
        onChange({
            ...group,
            filters: [
                ...group.filters,
                {
                    id,
                    field: field.field,
                    operator: operator ?? field.defaultOperator,
                    values,
                },
            ],
        });
        return id;
    };

    const addableFields = useMemo(() => fields.filter((field) => !field.hidden), [fields]);
    const updateMatch = (match: FilterMatchMode) => onChange({ ...group, match });
    const showConnector = group.filters.length + (group.groups?.length ?? 0) > 1;
    const addFilterPicker = <AddFilterPicker fields={fields} labels={labels} onUpsert={upsertFilter} />;
    const addFilterInline = addFilterPlacement === 'inline' || depth > 0;
    const hasRules = !!group.filters.length || !!group.groups?.length;
    const showMatch = depth > 0 || showConnector;

    return (
        <GroupContainer $nested={depth > 0}>
            {(addFilterInline || !!group.filters.length || showMatch) && (
                <FiltersRow data-testid="filter-bar-rules">
                    {showMatch && (
                        <MatchTabs
                            tabs={[
                                { key: 'all', label: labels.all, dataTestId: 'filter-bar-match-all' },
                                { key: 'any', label: labels.any, dataTestId: 'filter-bar-match-any' },
                            ]}
                            activeTab={group.match}
                            onTabClick={(key) => updateMatch(key as FilterMatchMode)}
                            fit="hug"
                            size="sm"
                            data-testid="filter-bar-match"
                            aria-label={`${labels.where} ${labels.all} or ${labels.any}`}
                        />
                    )}
                    {group.filters.map((rule, index) => {
                        const field = fields.find((candidate) => candidate.field === rule.field);
                        if (!field) return null;
                        return (
                            <RuleChip
                                key={rule.id}
                                rule={rule}
                                field={field}
                                labels={labels}
                                onChange={(updatedRule) => {
                                    const filters = [...group.filters];
                                    filters[index] = updatedRule;
                                    onChange({ ...group, filters });
                                }}
                                onRemove={() =>
                                    onChange({
                                        ...group,
                                        filters: group.filters.filter((filter) => filter.id !== rule.id),
                                    })
                                }
                            />
                        );
                    })}

                    {addFilterInline && addFilterPicker}
                    {onRemove && (
                        <GroupRemoveButton type="button" aria-label={labels.removeGroup} onClick={onRemove}>
                            <X size={14} />
                        </GroupRemoveButton>
                    )}
                </FiltersRow>
            )}

            {group.groups?.map((childGroup, index) => (
                <FilterGroupView
                    key={childGroup.id}
                    group={childGroup}
                    fields={fields}
                    labels={labels}
                    depth={depth + 1}
                    maxDepth={maxDepth}
                    allowGroups={allowGroups}
                    addFilterPlacement={addFilterPlacement}
                    onChange={(updatedGroup) => {
                        const groups = [...(group.groups ?? [])];
                        groups[index] = updatedGroup;
                        onChange({ ...group, groups });
                    }}
                    onRemove={() =>
                        onChange({
                            ...group,
                            groups: group.groups?.filter((candidate) => candidate.id !== childGroup.id),
                        })
                    }
                />
            ))}

            {((allowGroups && depth < maxDepth) ||
                (depth === 0 && (hasRules || (!addFilterInline && !!addableFields.length)))) && (
                <GroupActions data-testid="filter-bar-actions">
                    {!addFilterInline && addFilterPicker}
                    {allowGroups && depth < maxDepth && (
                        <GhostTrigger
                            type="button"
                            onClick={() =>
                                onChange({
                                    ...group,
                                    groups: [
                                        ...(group.groups ?? []),
                                        {
                                            id: createId('group'),
                                            match: 'all',
                                            filters: [],
                                        },
                                    ],
                                })
                            }
                        >
                            <Plus size={14} />
                            {labels.addGroup}
                        </GhostTrigger>
                    )}
                    {depth === 0 && hasRules && (
                        <GhostTrigger
                            type="button"
                            data-testid="filter-bar-clear-all"
                            onClick={() => onChange({ ...group, filters: [], groups: [] })}
                        >
                            {labels.clearAll}
                        </GhostTrigger>
                    )}
                </GroupActions>
            )}
        </GroupContainer>
    );
}

/**
 * Renders a progressively disclosed filter bar with editable field, operator,
 * and value chips. Nested groups are optional for advanced use cases.
 */
export function FilterBar({
    value,
    fields,
    onChange,
    allowGroups = false,
    addFilterPlacement = 'inline',
    menuPlacement = 'bottom',
    menuAnchor,
    onMenuOpenChange,
    labels: labelsOverride,
    maxDepth = 1,
    className,
}: FilterBarProps): JSX.Element {
    const labels = { ...DEFAULT_LABELS, ...labelsOverride };
    const openMenuCount = useRef(0);
    const onMenuOpenChangeRef = useRef(onMenuOpenChange);
    onMenuOpenChangeRef.current = onMenuOpenChange;
    const reportMenuOpen = useCallback((open: boolean) => {
        const wasOpen = openMenuCount.current > 0;
        openMenuCount.current += open ? 1 : -1;
        const isOpen = openMenuCount.current > 0;
        if (isOpen !== wasOpen) onMenuOpenChangeRef.current?.(isOpen);
    }, []);

    return (
        <MenuPlacementContext.Provider value={menuPlacement}>
            <MenuAnchorContext.Provider value={menuAnchor}>
                <MenuOpenContext.Provider value={reportMenuOpen}>
                    <Container className={className}>
                        <FilterGroupView
                            group={value}
                            fields={fields}
                            labels={labels}
                            depth={0}
                            maxDepth={maxDepth}
                            allowGroups={allowGroups}
                            addFilterPlacement={addFilterPlacement}
                            onChange={onChange}
                        />
                    </Container>
                </MenuOpenContext.Provider>
            </MenuAnchorContext.Provider>
        </MenuPlacementContext.Provider>
    );
}
