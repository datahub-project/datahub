import { Button, Icon, Input, Popover, SimpleSelect, spacing } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { FilterRecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import { SectionName } from '@app/ingestV2/source/multiStepBuilder/components/SectionName';
import { RemoveIcon } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/shared/RemoveIcon';
import RegexTooltipContent from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/filtersSection/RegexTooltipContent';
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

const FiltersGridContainer = styled.div`
    display: grid;
    grid-template-columns: 25% 25% 1fr auto;
    gap: ${spacing.md};
    align-items: start;
    width: 100%;
`;

const FilterHeaderCell = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const FilterCell = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.xsm};
`;

const RemoveIconCell = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100%;
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
                name="Asset Filters"
                description="Optional. Leave blank to ingest all accessible assets. Create include rules to allow specific assets, exclude rules to block them, or both."
                topRowRightItems={
                    <Button size="sm" onClick={onAddFilterClick}>
                        Add Filter
                    </Button>
                }
            />
            <FiltersGridContainer>
                {/* Header Row */}
                <FilterHeaderCell>
                    <FieldLabel label="Filter Type" />
                </FilterHeaderCell>
                <FilterHeaderCell>
                    <FieldLabel label="Asset Type" />
                </FilterHeaderCell>
                <FilterHeaderCell>
                    <FieldLabel label="Name or Pattern" />
                    <Popover content={<RegexTooltipContent />}>
                        <Icon icon="Info" source="phosphor" color="gray" size="lg" />
                    </Popover>
                </FilterHeaderCell>
                <div /> {/* Empty cell for the remove button column */}
                {/* Filter Rows */}
                {filters.map((filter) => (
                    <React.Fragment key={filter.key}>
                        <FilterCell>
                            <SimpleSelect
                                options={ruleSelectOptions}
                                values={filter.rule ? [filter.rule] : [defaultRule]}
                                onUpdate={(values) => updateFilterRule(filter.key, values?.[0])}
                                showClear={false}
                                width="full"
                                minWidth="fit-content"
                                placeholder="Rule"
                                size="lg"
                            />
                        </FilterCell>
                        <FilterCell>
                            <SimpleSelect
                                options={subtypeSelectOptions}
                                values={filter.subtype ? [filter.subtype] : defaultSubtypeSelectValues}
                                onUpdate={(values) => updateFilterSubtype(filter.key, values?.[0])}
                                showClear={false}
                                width="full"
                                minWidth="fit-content"
                                placeholder={filter.subtype ? `[${filter.subtype}]` : '[Table]'}
                                size="lg"
                            />
                        </FilterCell>
                        <FilterCell>
                            <Input
                                value={filter.value}
                                setValue={(value) => updateFilterValue(filter.key, value)}
                                placeholder="^my_db$"
                            />
                        </FilterCell>
                        <RemoveIconCell>
                            <RemoveIcon onClick={() => removeFilter(filter.key)} />
                        </RemoveIconCell>
                    </React.Fragment>
                ))}
            </FiltersGridContainer>
        </>
    );
}
