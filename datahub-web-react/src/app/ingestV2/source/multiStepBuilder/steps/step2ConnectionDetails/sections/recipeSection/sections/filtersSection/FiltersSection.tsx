import { Button, Input, SimpleSelect, spacing } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { FilterRecipeField, FilterRule } from '@app/ingestV2/source/builder/RecipeForm/common';
import { FieldWrapper } from '@app/ingestV2/source/multiStepBuilder/components/FieldWrapper';
import { SectionName } from '@app/ingestV2/source/multiStepBuilder/components/SectionName';
import { RemoveIcon } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/shared/RemoveIcon';
import { Filter } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/filtersSection/types';
import {
    convertFiltersToFieldValues,
    filterOutUnsupportedFields,
    getEmptyFilter,
    getInitialFilters,
    getOptionsForTypeSelect,
    getSubtypeOptions,
} from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/filtersSection/utils';

const FilterRow = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: ${spacing.lg};
    margin-left: 1px;
    width: 100%;
`;

const FilterFieldsWrapper = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: ${spacing.md};
    width: 100%;
    align-items: start;
`;

const SelectWrapper = styled.div`
    width: 25%;
`;

interface Props {
    fields: FilterRecipeField[];
    recipe: string;
    updateRecipe: (fieldNames: Record<string, string[]>, values: Record<string, string[]>) => void;
}

export function FiltersSection({ fields, recipe, updateRecipe }: Props) {
    const supportedFields = useMemo(() => filterOutUnsupportedFields(fields), [fields]);
    const ruleSelectOptions = useMemo(() => getOptionsForTypeSelect(), []);
    // FYI: assuming that each filter has both allow and deny version
    const subtypeSelectOptions = useMemo(() => getSubtypeOptions(supportedFields), [supportedFields]);
    const defaultRule = useMemo(() => {
        if (ruleSelectOptions.length > 0) {
            return ruleSelectOptions[0].value;
        }
        return undefined;
    }, [ruleSelectOptions]);

    const defaultSubtype = useMemo(() => {
        if (subtypeSelectOptions.length > 0) {
            return subtypeSelectOptions[0].value;
        }
        return undefined;
    }, [subtypeSelectOptions]);

    const defaultsForEmptyFilter = useMemo(
        () => ({
            rule: defaultRule,
            subtype: defaultSubtype,
        }),
        [defaultRule, defaultSubtype],
    );

    const [filters, setFilters] = useState<Filter[]>(() =>
        getInitialFilters(supportedFields, recipe, defaultsForEmptyFilter),
    );

    const addFilter = useCallback(() => {
        setFilters((prev) => [...prev, getEmptyFilter(defaultsForEmptyFilter)]);
    }, [defaultsForEmptyFilter]);

    const onAddFilterClick = useCallback(
        (e: React.MouseEvent) => {
            e.stopPropagation();
            e.preventDefault();
            addFilter();
        },
        [addFilter],
    );

    const updateRecipeByFilters = useCallback(
        (updatedFilters: Filter[]) => {
            const values = convertFiltersToFieldValues(updatedFilters, fields);
            updateRecipe(values, values);
        },
        [updateRecipe, fields],
    );

    const removeFilter = useCallback(
        (key: string) => {
            const updatedFilters = filters.filter((filter) => filter.key !== key);
            if (updatedFilters.length === 0) {
                setFilters([getEmptyFilter(defaultsForEmptyFilter)]);
            } else {
                setFilters(updatedFilters);
            }
            updateRecipeByFilters(updatedFilters);
        },
        [filters, updateRecipeByFilters, defaultsForEmptyFilter],
    );

    const updateFilters = useCallback(
        (key: string, filterChanges: Partial<Filter>) => {
            const updatedFilters = filters.map((filter) => {
                if (filter.key === key) {
                    return { ...filter, ...filterChanges };
                }
                return filter;
            });

            setFilters(updatedFilters);
            updateRecipeByFilters(updatedFilters);
        },
        [filters, updateRecipeByFilters],
    );

    const updateFilterRule = useCallback(
        (key: string, value: string | undefined) => {
            updateFilters(key, { rule: value });
        },
        [updateFilters],
    );

    const updateFilterSubtype = useCallback(
        (key: string, value: string | undefined) => {
            updateFilters(key, { subtype: value });
        },
        [updateFilters],
    );

    const updateFilterValue = useCallback(
        (key: string, value: string | undefined) => {
            updateFilters(key, { value: value || '' });
        },
        [updateFilters],
    );

    return (
        <>
            <SectionName
                name="Filters"
                topRowRightItems={
                    <Button size="sm" onClick={onAddFilterClick}>
                        Add Filter
                    </Button>
                }
            />
            {filters.map((filter) => (
                <FilterRow>
                    <FilterFieldsWrapper>
                        <SelectWrapper>
                            <FieldWrapper label="Rule" help="Include or exclude matching entities">
                                <SimpleSelect
                                    options={ruleSelectOptions}
                                    values={filter.rule ? [filter.rule] : [FilterRule.INCLUDE]}
                                    onUpdate={(values) => updateFilterRule(filter.key, values?.[0])}
                                    showClear={false}
                                    width="full"
                                    placeholder="Rule"
                                    size="lg"
                                />
                            </FieldWrapper>
                        </SelectWrapper>
                        <SelectWrapper>
                            <FieldWrapper label="Subtype" required help="Type of entity to filter">
                                <SimpleSelect
                                    options={subtypeSelectOptions}
                                    values={filter.subtype ? [filter.subtype] : [subtypeSelectOptions?.[0].value]}
                                    onUpdate={(values) => updateFilterSubtype(filter.key, values?.[0])}
                                    showClear={false}
                                    width="full"
                                    placeholder="[Table]"
                                    size="lg"
                                />
                            </FieldWrapper>
                        </SelectWrapper>
                        <FieldWrapper
                            label="Regex Entry"
                            help="Regular expressions (regex) for pattern matching within strings"
                        >
                            <Input
                                value={filter.value}
                                setValue={(value) => updateFilterValue(filter.key, value)}
                                placeholder='apple: Matches the literal string "apple"'
                            />
                        </FieldWrapper>
                    </FilterFieldsWrapper>
                    <RemoveIcon onClick={() => removeFilter(filter.key)} />
                </FilterRow>
            ))}
        </>
    );
}
