import { Checkbox, Input, Loader } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useMemo, useRef, useState } from 'react';

import {
    AddFilterMenu,
    Chip,
    ChipSegment,
    Container,
    ExpandToggle,
    ExpandToggleSpacer,
    FieldName,
    FilterPopover,
    FiltersRow,
    GhostTrigger,
    GroupActions,
    GroupContainer,
    GroupHeader,
    MatchButton,
    MatchControls,
    MenuState,
    NestedOptionIndent,
    OptionCheckboxSlot,
    OptionContent,
    OptionCount,
    OptionDescription,
    OptionLabel,
    OptionList,
    OptionRow,
    RemoveButton,
    ValueFlyoutPanel,
    ValueIconStack,
    ValueIconStackItem,
} from '@components/components/FilterBar/components';
import {
    FilterBarLabels,
    FilterBarProps,
    FilterField,
    FilterGroup,
    FilterMatchMode,
    FilterRule,
    FilterValueOption,
} from '@components/components/FilterBar/types';

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

function getOperator(field: FilterField, value: string) {
    return field.operators.find((operator) => operator.value === value) ?? field.operators[0];
}

function getSelectedOptions(field: FilterField, values: string[]): FilterValueOption[] {
    const availableOptions = [...(field.values ?? []), ...(field.selectedOptions ?? [])];
    const flattened = flattenFilterOptions(availableOptions);
    return values.map((value) => flattened.find((option) => option.value === value) ?? { value, label: value });
}

function getValueLabel(selectedOptions: FilterValueOption[], placeholder: string): string {
    if (!selectedOptions.length) return placeholder;
    const labels = selectedOptions.map((option) => option.label);
    if (labels.length <= 2) return labels.join(', ');
    return `${labels.slice(0, 2).join(', ')} +${labels.length - 2}`;
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

function BuiltInValueEditor({ rule, field, labels, trigger, onChange, compose }: ValueEditorProps) {
    const isCompose = !!compose;
    const [isOpen, setIsOpen] = useState(false);
    const [query, setQuery] = useState('');
    const [draftValues, setDraftValues] = useState<string[]>(rule.values);
    const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set());

    const activeValues = isCompose ? rule.values : draftValues;

    const options = useMemo(() => {
        const fieldValues = [
            ...(field.values ?? []),
            ...(field.selectedOptions ?? []).filter(
                (selectedOption) =>
                    !flattenFilterOptions(field.values ?? []).some((option) => option.value === selectedOption.value),
            ),
        ];
        if (field.onSearch || !query) return fieldValues;
        return filterOptionsByQuery(fieldValues, query);
    }, [field.onSearch, field.selectedOptions, field.values, query]);

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
        const valuesChanged =
            draftValues.length !== rule.values.length ||
            draftValues.some((value) => !rule.values.includes(value)) ||
            rule.values.some((value) => !draftValues.includes(value));
        if (valuesChanged) {
            onChange({ ...rule, values: draftValues });
        }
        setIsOpen(false);
        setQuery('');
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
            setQuery('');
            return;
        }

        const descendantValues = collectDescendantValues(option);
        const isSelected = activeValues.includes(option.value);
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
                    {option.count !== undefined && <OptionCount>{option.count.toLocaleString()}</OptionCount>}
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
                    icon={{ icon: MagnifyingGlass }}
                    onClear={() => {
                        setQuery('');
                        field.onSearch?.('');
                    }}
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

type OperatorEditorProps = {
    rule: FilterRule;
    field: FilterField;
    onChange: (rule: FilterRule) => void;
};

function OperatorEditor({ rule, field, onChange }: OperatorEditorProps) {
    const [isOpen, setIsOpen] = useState(false);
    const operator = getOperator(field, rule.operator);
    const displayLabel = rule.values.length > 1 && operator?.pluralLabel ? operator.pluralLabel : operator?.label;
    // "is all of" only applies with 2+ values (same as legacy ALL_EQUALS).
    const visibleOperators = field.operators.filter(
        (option) => option.value !== 'is_all' || rule.values.length > 1 || rule.operator === 'is_all',
    );

    return (
        <FilterPopover
            isOpen={isOpen}
            onClose={() => setIsOpen(false)}
            width={220}
            trigger={
                <ChipSegment type="button" onClick={() => setIsOpen(!isOpen)}>
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
    const committedRuleIdRef = useRef<string | null>(null);
    committedRuleIdRef.current = committedRuleId;

    const { rootFields, groups } = useMemo(() => {
        const nextRoot: FilterField[] = [];
        const nextGroups = new Map<string, FilterField[]>();

        // Preserve caller order (SORTED_FILTERS priority) — do not alphabetize.
        fields.forEach((field) => {
            if (!field.group) {
                nextRoot.push(field);
                return;
            }
            nextGroups.set(field.group, [...(nextGroups.get(field.group) ?? []), field]);
        });

        return { rootFields: nextRoot, groups: nextGroups };
    }, [fields]);

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
        // Wait for a full date range before creating a chip.
        if (rule.operator === 'between' && rule.values.length < 2 && !activeRuleId) {
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
        if (pendingField?.field === field.field && composeRule) return;
        setPendingField(field);
        committedRuleIdRef.current = null;
        setCommittedRuleId(null);
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

    const renderFieldRow = (
        field: FilterField,
        options?: { description?: string; onHover?: (field: FilterField) => void },
    ) => (
        <OptionRow
            key={field.field}
            type="button"
            $active={pendingField?.field === field.field}
            onMouseEnter={() => (options?.onHover ?? previewCompose)(field)}
            onFocus={() => (options?.onHover ?? previewCompose)(field)}
            onClick={() => onFieldActivate(field)}
        >
            <OptionContent>
                <OptionLabel>{field.label}</OptionLabel>
                {(options?.description || field.description) && (
                    <OptionDescription>{options?.description || field.description}</OptionDescription>
                )}
            </OptionContent>
            <CaretRight size={14} />
        </OptionRow>
    );

    return (
        <FilterPopover
            isOpen={isOpen}
            onClose={close}
            trigger={
                <GhostTrigger type="button" onClick={() => (isOpen ? close() : setIsOpen(true))}>
                    <Plus size={14} />
                    {labels.addFilter}
                </GhostTrigger>
            }
        >
            <AddFilterMenu>
                <Input
                    value={query}
                    setValue={(value) => {
                        setQuery(value);
                        setActiveGroup(null);
                        resetCompose();
                    }}
                    placeholder={labels.searchFilters}
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
                                  $active={activeGroup === group}
                                  onMouseEnter={() => previewGroup(group)}
                                  onFocus={() => previewGroup(group)}
                                  onClick={() => previewGroup(group)}
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
                    /* eslint-disable-next-line i18next/no-literal-string -- ARIA role, not UI copy */
                    <ValueFlyoutPanel role="dialog" aria-label={activeGroup ?? undefined}>
                        <OptionList>
                            {activeGroupFields.map((field) => renderFieldRow(field))}
                            {!activeGroupFields.length && <MenuState>{labels.noFilters}</MenuState>}
                        </OptionList>
                        {showGroupValueFlyout && composeField && (
                            /* eslint-disable-next-line i18next/no-literal-string -- ARIA role, not UI copy */
                            <ValueFlyoutPanel role="dialog" aria-label={composeField.label}>
                                <React.Fragment key={composeField.field}>
                                    {renderValueCompose(composeField)}
                                </React.Fragment>
                            </ValueFlyoutPanel>
                        )}
                    </ValueFlyoutPanel>
                )}
                {showRootValueFlyout && composeField && (
                    /* eslint-disable-next-line i18next/no-literal-string -- ARIA role, not UI copy */
                    <ValueFlyoutPanel role="dialog" aria-label={composeField.label}>
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
    const stackedIcons = selectedOptions.filter((option) => option.icon).slice(0, 4);
    const valueTrigger = (
        <ChipSegment type="button" $emphasized $placeholder={!rule.values.length}>
            {!!stackedIcons.length && (
                <ValueIconStack>
                    {stackedIcons.map((option) => (
                        <ValueIconStackItem key={option.value}>{option.icon}</ValueIconStackItem>
                    ))}
                </ValueIconStack>
            )}
            {getValueLabel(selectedOptions, labels.chooseValue)}
        </ChipSegment>
    );

    return (
        <Chip>
            <FieldName>{field.label}</FieldName>
            <OperatorEditor rule={rule} field={field} onChange={onChange} />
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
    onChange: (group: FilterGroup) => void;
    onRemove?: () => void;
};

function FilterGroupView({ group, fields, labels, depth, maxDepth, allowGroups, onChange, onRemove }: GroupProps) {
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
        if (!values.length) {
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

    const updateMatch = (match: FilterMatchMode) => onChange({ ...group, match });
    const showConnector = group.filters.length + (group.groups?.length ?? 0) > 1;

    return (
        <GroupContainer $nested={depth > 0}>
            {(depth > 0 || showConnector) && (
                <GroupHeader>
                    <MatchControls>
                        {labels.where}
                        <MatchButton
                            type="button"
                            aria-label={`${labels.where} ${labels.all} or ${labels.any}`}
                            onClick={() => updateMatch(group.match === 'all' ? 'any' : 'all')}
                        >
                            {group.match === 'all' ? labels.all : labels.any}
                        </MatchButton>
                        {depth > 0 && <span>:</span>}
                    </MatchControls>
                    {onRemove && (
                        <RemoveButton type="button" aria-label={labels.removeGroup} onClick={onRemove}>
                            <X size={14} />
                        </RemoveButton>
                    )}
                </GroupHeader>
            )}

            <FiltersRow>
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

                {!!fields.length && <AddFilterPicker fields={fields} labels={labels} onUpsert={upsertFilter} />}
            </FiltersRow>

            {group.groups?.map((childGroup, index) => (
                <FilterGroupView
                    key={childGroup.id}
                    group={childGroup}
                    fields={fields}
                    labels={labels}
                    depth={depth + 1}
                    maxDepth={maxDepth}
                    allowGroups={allowGroups}
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
                (depth === 0 && (!!group.filters.length || !!group.groups?.length))) && (
                <GroupActions>
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
                    {depth === 0 && (!!group.filters.length || !!group.groups?.length) && (
                        <GhostTrigger type="button" onClick={() => onChange({ ...group, filters: [], groups: [] })}>
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
    labels: labelsOverride,
    maxDepth = 1,
    className,
}: FilterBarProps): JSX.Element {
    const labels = { ...DEFAULT_LABELS, ...labelsOverride };

    return (
        <Container className={className}>
            <FilterGroupView
                group={value}
                fields={fields}
                labels={labels}
                depth={0}
                maxDepth={maxDepth}
                allowGroups={allowGroups}
                onChange={onChange}
            />
        </Container>
    );
}
