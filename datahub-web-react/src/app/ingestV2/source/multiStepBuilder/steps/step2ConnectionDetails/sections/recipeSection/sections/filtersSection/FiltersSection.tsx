import { Button, Input, SimpleSelect, spacing } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { FilterRecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
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
import { FieldLabel } from '@app/sharedV2/forms/FieldLabel';

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

const SelectLabelWrapper = styled.div`
    min-width: 175px;
    width: 25%;
`;

const SelectLabelWrapperFullWidth = styled.div`
    width: 100%;
`;

const Spacer = styled.div`
    width: 16px;
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
        return 'exclude';
    }, []);

    const defaultSubtype = useMemo(() => {
        if (subtypeSelectOptions.length > 0) {
            return subtypeSelectOptions[0].value;
        }
        return undefined;
    }, [subtypeSelectOptions]);

    const defaultSubtypeSelectValues = useMemo(() => {
        if (defaultSubtype) {
            return [defaultSubtype];
        }
        return [];
    }, [defaultSubtype]);

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

    if (fields.length === 0) return null;

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
            <FilterRow>
                <FilterFieldsWrapper>
                    <SelectLabelWrapper>
                        <FieldLabel label="Rule" />
                    </SelectLabelWrapper>
                    <SelectLabelWrapper>
                        <FieldLabel label="Subtype" />
                    </SelectLabelWrapper>
                    <SelectLabelWrapperFullWidth>
                        <FieldLabel label="Regex Entry" />
                    </SelectLabelWrapperFullWidth>
                </FilterFieldsWrapper>
                <Spacer />
            </FilterRow>
            {filters.map((filter) => (
                <FilterRow>
                    <FilterFieldsWrapper>
                        <SelectWrapper>
                            <SimpleSelect
                                options={ruleSelectOptions}
                                values={filter.rule ? [filter.rule] : [defaultRule]}
                                onUpdate={(values) => updateFilterRule(filter.key, values?.[0])}
                                showClear={false}
                                width="full"
                                placeholder="Rule"
                                size="lg"
                            />
                        </SelectWrapper>
                        <SelectWrapper>
                            <SimpleSelect
                                options={subtypeSelectOptions}
                                values={filter.subtype ? [filter.subtype] : defaultSubtypeSelectValues}
                                onUpdate={(values) => updateFilterSubtype(filter.key, values?.[0])}
                                showClear={false}
                                width="full"
                                placeholder={filter.subtype ? `[${filter.subtype}]` : '[Table]'}
                                size="lg"
                            />
                        </SelectWrapper>
                        <Input
                            value={filter.value}
                            setValue={(value) => updateFilterValue(filter.key, value)}
                            placeholder="^my_db$"
                        />
                    </FilterFieldsWrapper>
                    <RemoveIcon onClick={() => removeFilter(filter.key)} />
                </FilterRow>
            ))}
        </>
    );
}
