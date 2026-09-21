import { Checkbox, Input, Loader } from '@components';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useMemo, useState } from 'react';

import {
    Chip,
    ChipSegment,
    Container,
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
    OptionContent,
    OptionCount,
    OptionDescription,
    OptionLabel,
    OptionList,
    OptionRow,
    RemoveButton,
    SectionLabel,
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
    chooseValue: 'Choose value',
    clearAll: 'Clear all',
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
    return values.map((value) => availableOptions.find((option) => option.value === value) ?? { value, label: value });
}

function getValueLabel(selectedOptions: FilterValueOption[], placeholder: string): string {
    if (!selectedOptions.length) return placeholder;
    const labels = selectedOptions.map((option) => option.label);
    if (labels.length <= 2) return labels.join(', ');
    return `${labels.slice(0, 2).join(', ')} +${labels.length - 2}`;
}

type ValueEditorProps = {
    rule: FilterRule;
    field: FilterField;
    labels: FilterBarLabels;
    trigger: React.ReactElement;
    onChange: (rule: FilterRule) => void;
};

function BuiltInValueEditor({ rule, field, labels, trigger, onChange }: ValueEditorProps) {
    const [isOpen, setIsOpen] = useState(false);
    const [query, setQuery] = useState('');
    const options = useMemo(() => {
        const fieldValues = [
            ...(field.values ?? []),
            ...(field.selectedOptions ?? []).filter(
                (selectedOption) => !field.values?.some((option) => option.value === selectedOption.value),
            ),
        ];
        // Fields that search server side already return the matches for the query.
        if (field.onSearch || !query) return fieldValues;
        const normalizedQuery = query.toLocaleLowerCase();
        return fieldValues.filter(
            (option) =>
                option.label.toLocaleLowerCase().includes(normalizedQuery) ||
                option.description?.toLocaleLowerCase().includes(normalizedQuery),
        );
    }, [field.onSearch, field.selectedOptions, field.values, query]);
    const selectableOptions = options.filter((option) => !option.disabled);
    const areAllVisibleSelected =
        selectableOptions.length > 0 && selectableOptions.every((option) => rule.values.includes(option.value));

    const close = () => {
        setIsOpen(false);
        setQuery('');
    };

    const updateValues = (values: string[]) => onChange({ ...rule, values });

    const toggleValue = (option: FilterValueOption) => {
        if (option.disabled) return;
        if (field.selectionMode === 'single') {
            updateValues([option.value]);
            close();
            return;
        }
        updateValues(
            rule.values.includes(option.value)
                ? rule.values.filter((value) => value !== option.value)
                : [...rule.values, option.value],
        );
    };

    const toggleAllVisible = () =>
        updateValues(
            areAllVisibleSelected
                ? rule.values.filter((value) => !selectableOptions.some((option) => option.value === value))
                : Array.from(new Set([...rule.values, ...selectableOptions.map((option) => option.value)])),
        );

    const onScroll = (event: React.UIEvent<HTMLDivElement>) => {
        if (!field.hasMore || field.loading) return;
        const { scrollHeight, scrollTop, clientHeight } = event.currentTarget;
        if (scrollHeight - scrollTop - clientHeight < LOAD_MORE_THRESHOLD_PX) field.onLoadMore?.();
    };

    return (
        <FilterPopover
            isOpen={isOpen}
            onClose={close}
            trigger={React.cloneElement(trigger, { onClick: () => (isOpen ? close() : setIsOpen(true)) })}
        >
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
            {field.showSelectAll && field.selectionMode !== 'single' && !!selectableOptions.length && (
                <OptionRow type="button" onClick={toggleAllVisible}>
                    <Checkbox
                        isChecked={areAllVisibleSelected}
                        isIntermediate={
                            !areAllVisibleSelected &&
                            selectableOptions.some((option) => rule.values.includes(option.value))
                        }
                        onCheckboxChange={toggleAllVisible}
                        size="sm"
                    />
                    <OptionContent>{labels.selectAll}</OptionContent>
                </OptionRow>
            )}
            <OptionList onScroll={onScroll}>
                {options.map((option) => (
                    <OptionRow
                        key={option.value}
                        type="button"
                        disabled={option.disabled}
                        onClick={() => toggleValue(option)}
                    >
                        {field.selectionMode !== 'single' && (
                            <Checkbox
                                isChecked={rule.values.includes(option.value)}
                                onCheckboxChange={() => toggleValue(option)}
                                isDisabled={option.disabled}
                                size="sm"
                            />
                        )}
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
                        {field.selectionMode === 'single' && rule.values.includes(option.value) && <Check size={16} />}
                    </OptionRow>
                ))}
                {field.loading && (
                    <MenuState>
                        <Loader size="sm" />
                    </MenuState>
                )}
                {!field.loading && !options.length && <MenuState>{labels.noFilters}</MenuState>}
            </OptionList>
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

    return (
        <FilterPopover
            isOpen={isOpen}
            onClose={() => setIsOpen(false)}
            width={220}
            trigger={
                <ChipSegment type="button" onClick={() => setIsOpen(!isOpen)}>
                    {operator?.label}
                </ChipSegment>
            }
        >
            <OptionList>
                {field.operators.map((option) => (
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
                        <OptionContent>{option.label}</OptionContent>
                        {option.value === rule.operator && <Check size={16} />}
                    </OptionRow>
                ))}
            </OptionList>
        </FilterPopover>
    );
}

type AddFilterPickerProps = {
    fields: FilterField[];
    labels: FilterBarLabels;
    onSelect: (field: FilterField) => void;
};

function AddFilterPicker({ fields, labels, onSelect }: AddFilterPickerProps) {
    const [isOpen, setIsOpen] = useState(false);
    const [query, setQuery] = useState('');
    const groupedFields = useMemo(() => {
        const normalizedQuery = query.trim().toLocaleLowerCase();
        const matchingFields = normalizedQuery
            ? fields.filter(
                  (field) =>
                      field.label.toLocaleLowerCase().includes(normalizedQuery) ||
                      field.description?.toLocaleLowerCase().includes(normalizedQuery) ||
                      field.group?.toLocaleLowerCase().includes(normalizedQuery),
              )
            : fields;

        return Array.from(
            matchingFields.reduce((groups, field) => {
                const group = field.group ?? '';
                groups.set(group, [...(groups.get(group) ?? []), field]);
                return groups;
            }, new Map<string, FilterField[]>()),
        );
    }, [fields, query]);

    const close = () => {
        setIsOpen(false);
        setQuery('');
    };

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
            <Input
                value={query}
                setValue={setQuery}
                placeholder={labels.searchFilters}
                icon={{ icon: MagnifyingGlass }}
                onClear={() => setQuery('')}
            />
            <OptionList>
                {groupedFields.map(([group, groupFields]) => (
                    <React.Fragment key={group || 'ungrouped'}>
                        {group && <SectionLabel>{group}</SectionLabel>}
                        {groupFields.map((field) => (
                            <OptionRow
                                key={field.field}
                                type="button"
                                onClick={() => {
                                    onSelect(field);
                                    close();
                                }}
                            >
                                <OptionContent>
                                    <OptionLabel>{field.label}</OptionLabel>
                                    {field.description && <OptionDescription>{field.description}</OptionDescription>}
                                </OptionContent>
                            </OptionRow>
                        ))}
                    </React.Fragment>
                ))}
                {!groupedFields.length && <MenuState>{labels.noFilters}</MenuState>}
            </OptionList>
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
    const usedFields = new Set(group.filters.map((filter) => filter.field));
    const availableFields = fields.filter((field) => !usedFields.has(field.field));
    const addFilter = (field: FilterField) =>
        onChange({
            ...group,
            filters: [
                ...group.filters,
                {
                    id: createId('filter'),
                    field: field.field,
                    operator: field.defaultOperator,
                    values: [],
                },
            ],
        });

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

                {!!availableFields.length && (
                    <AddFilterPicker fields={availableFields} labels={labels} onSelect={addFilter} />
                )}
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
