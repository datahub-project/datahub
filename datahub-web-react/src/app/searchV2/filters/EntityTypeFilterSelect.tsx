import { Pill } from '@components';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import { NestedSelect } from '@components/components/Select/Nested/NestedSelect';
import { NestedSelectOption, RenderOptionProps } from '@components/components/Select/Nested/types';
import spacing from '@components/theme/foundations/spacing';

import { FilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import {
    deduplicateOptions,
    useFilterOptionsBySearchQuery,
    useLoadAggregationOptions,
} from '@app/searchV2/filters/value/utils';
import { FILTER_DELIMITER } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { EntityType } from '@src/types.generated';

const OptionWrapper = styled.div`
    display: flex;
    gap: ${spacing.xsm};
    justify-content: space-between;
    width: 100%;
`;
const OriginOptionWrapper = styled.div`
    display: flex;
`;

interface EntityTypeSelectOption extends NestedSelectOption {
    counter?: number;
}

interface Props {
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    includeSubTypes?: boolean;
    includeCount?: boolean;
    aggregationsEntityTypes?: Array<EntityType>;
}

export default function EntityTypeFilterSelect({
    field,
    values,
    defaultOptions,
    onChangeValues,
    includeSubTypes = true,
    includeCount = false,
    aggregationsEntityTypes,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const { displayName } = field;

    const selectedValues = useMemo(() => values.map((v) => v.value), [values]);

    const { options: aggOptions, loading: aggLoading } = useLoadAggregationOptions({
        field,
        visible: true,
        includeCounts: includeCount,
        aggregationsEntityTypes,
    });

    const allOptions = [...defaultOptions, ...deduplicateOptions(defaultOptions, aggOptions)];

    const [searchQuery, setSearchQuery] = React.useState<string | undefined>(undefined);
    const localSearchOptions = useFilterOptionsBySearchQuery(allOptions, searchQuery);
    const finalOptions = searchQuery ? localSearchOptions : allOptions;

    const nestedOptions: EntityTypeSelectOption[] = useMemo(() => {
        const rootOptions = finalOptions.filter((option) => !option.value.includes(FILTER_DELIMITER));

        const options: EntityTypeSelectOption[] = [];

        rootOptions.forEach((option) => {
            const label = entityRegistry.getEntityName(option.value as EntityType) || option.value;

            const hasChildren =
                includeSubTypes &&
                finalOptions.some(
                    (opt) =>
                        opt.value.includes(FILTER_DELIMITER) && opt.value.startsWith(option.value + FILTER_DELIMITER),
                );

            options.push({
                value: option.value,
                label: label || option.displayName || option.value,
                isParent: hasChildren,
                entity: option.entity,
                counter: includeCount ? option.count : undefined,
            });

            if (includeSubTypes && hasChildren) {
                const children = finalOptions.filter(
                    (opt) =>
                        opt.value.includes(FILTER_DELIMITER) && opt.value.startsWith(option.value + FILTER_DELIMITER),
                );

                children.forEach((childOption) => {
                    const [, subType] = childOption.value.split(FILTER_DELIMITER);
                    options.push({
                        value: childOption.value,
                        label: childOption.displayName || subType,
                        parentValue: option.value,
                        entity: childOption.entity,
                        counter: includeCount ? childOption.count : undefined,
                    });
                });
            }
        });

        return options;
    }, [finalOptions, includeSubTypes, includeCount, entityRegistry]);

    const initialValues = useMemo(() => {
        return nestedOptions.filter((opt) => selectedValues.includes(opt.value));
    }, [nestedOptions, selectedValues]);

    const handleUpdate = useCallback(
        (selectedOptions: NestedSelectOption[]) => {
            const newValues = selectedOptions.map((opt) => opt.value);
            onChangeValues(
                newValues.map((value) => ({
                    field: field.field,
                    value,
                    entity: null,
                })),
            );
        },
        [onChangeValues, field.field],
    );

    const handleSearch = useCallback((query: string) => {
        setSearchQuery(query || undefined);
    }, []);

    const renderCustomOption = useCallback(({ option, origin }: RenderOptionProps<EntityTypeSelectOption>) => {
        return (
            <OptionWrapper>
                <OriginOptionWrapper>{origin}</OriginOptionWrapper>
                {option.counter !== undefined && <Pill label={`${option.counter}`} size="xs" />}
            </OptionWrapper>
        );
    }, []);

    return (
        <NestedSelect
            options={nestedOptions}
            initialValues={initialValues}
            values={selectedValues}
            onUpdate={handleUpdate}
            onSearch={handleSearch}
            isActive={initialValues.length > 0}
            isMultiSelect
            width="fit-content"
            dataTestId="filter-entity-type"
            size="sm"
            showSearch
            searchPlaceholder={`Search ${displayName}`}
            renderCustomOptionText={(option) => <span>{option.label}</span>}
            isLoadingParentChildList={aggLoading}
            selectLabelProps={{ variant: 'labeled', label: displayName }}
            renderCustomOption={renderCustomOption}
            shouldUpdateValuesOnClose
            showClear
        />
    );
}
