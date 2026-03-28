import { Checkbox, Pill } from '@components';
import { CaretUp } from '@phosphor-icons/react/dist/csr/CaretUp';
import React, { useState } from 'react';
import styled from 'styled-components';

import { generateColor } from '@app/entityV2/shared/components/styled/StyledTag';
import ParentEntities from '@app/searchV2/filters/ParentEntities';
import { Label } from '@app/searchV2/filters/styledComponents';
import { FilterOptionType } from '@app/searchV2/filters/types';
import {
    getFilterIconAndLabel,
    getParentEntities,
    isAnyOptionSelected,
    isFilterOptionSelected,
} from '@app/searchV2/filters/utils';
import {
    DOMAINS_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    MAX_COUNT_VAL,
    TYPE_NAMES_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import { formatNumber } from '@app/shared/formatNumber';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { EntitySelectOption } from '@app/sharedV2/select/EntitySelectOption';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType } from '@types';

const FilterOptionWrapper = styled.div<{ addPadding?: boolean }>`
    display: flex;
    align-items: center;
    border-radius: 8px;
    padding: 8px 4px;
    gap: 4px;
    cursor: pointer;
    font-size: 14px;

    ${(props) => props.addPadding && 'padding-left: 24px;'}

    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
`;

const CheckboxContent = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    overflow: hidden;
    flex: 1;
`;

export const TagColor = styled.span<{ color: string; colorHash?: string | null }>`
    height: 8px;
    width: 8px;
    border-radius: 50%;
    background-color: ${(props) => (props.color ? props.color : generateColor.hex(props.colorHash))};
    margin-right: 3px;
`;

const LabelCountWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const ArrowButton = styled.button<{ $isOpen: boolean }>`
    margin-left: 4px;
    background: none;
    border: none;
    cursor: pointer;
    padding: 2px;
    display: flex;
    align-items: center;
    color: ${(props) => props.theme.colors.icon};
    transition: transform 0.2s ease;
    ${(props) => props.$isOpen && 'transform: rotate(180deg);'}

    &:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

const ParentWrapper = styled.div`
    max-width: 220px;
    font-size: 12px;
`;

const LabelWrapper = styled.div`
    line-height: normal;
`;

interface Props {
    filterOption: FilterOptionType;
    selectedFilterOptions: FilterOptionType[];
    setSelectedFilterOptions: (filterValues: FilterOptionType[]) => void;
    nestedOptions?: FilterOptionType[];
    addPadding?: boolean;
    includeCount?: boolean;
}

export default function FilterOption({
    filterOption,
    selectedFilterOptions,
    setSelectedFilterOptions,
    nestedOptions,
    addPadding,
    includeCount = true,
}: Props) {
    const [areChildrenVisible, setAreChildrenVisible] = useState(true);
    const { field, value, count, entity } = filterOption;
    const entityRegistry = useEntityRegistry();
    const { label } = getFilterIconAndLabel(field, value, entityRegistry, entity || null, 14, filterOption.displayName);
    const showParentEntityPath = field === DOMAINS_FILTER_NAME && entity?.type === EntityType.Domain;
    const isSubTypeFilter = field === TYPE_NAMES_FILTER_NAME;
    const parentEntities: Entity[] = getParentEntities(entity as Entity) || [];
    const isSelected = isFilterOptionSelected(selectedFilterOptions, value);
    const isIndeterminate = isAnyOptionSelected(
        selectedFilterOptions,
        nestedOptions?.map((o) => o.value),
    );

    const getCountText = () => {
        return count === MAX_COUNT_VAL && field === ENTITY_SUB_TYPE_FILTER_NAME ? '10k+' : formatNumber(count);
    };

    function updateFilterValues() {
        if (isFilterOptionSelected(selectedFilterOptions, value)) {
            setSelectedFilterOptions(selectedFilterOptions.filter((option) => option.value !== value));
        } else {
            const filteredSelectedOptions = selectedFilterOptions.filter(
                (o) => !nestedOptions?.some((nestedOption) => nestedOption.value === o.value),
            );
            setSelectedFilterOptions([...filteredSelectedOptions, filterOption]);
        }
    }

    return (
        <>
            <FilterOptionWrapper addPadding={addPadding} onClick={updateFilterValues}>
                <CheckboxContent>
                    <LabelWrapper className="test-label">
                        {!showParentEntityPath && parentEntities.length > 0 && (
                            <ParentWrapper>
                                <ParentEntities parentEntities={parentEntities} />
                            </ParentWrapper>
                        )}
                        <LabelCountWrapper>
                            {filterOption.entity ? (
                                <EntitySelectOption entity={filterOption.entity} />
                            ) : (
                                <Label style={{ maxWidth: 150 }}>
                                    {isSubTypeFilter ? capitalizeFirstLetterOnly(label as string) : label}
                                </Label>
                            )}
                            {nestedOptions && nestedOptions.length > 0 && (
                                <ArrowButton
                                    $isOpen={areChildrenVisible}
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        setAreChildrenVisible(!areChildrenVisible);
                                    }}
                                >
                                    <CaretUp size={18} />
                                </ArrowButton>
                            )}
                        </LabelCountWrapper>
                    </LabelWrapper>
                </CheckboxContent>
                {includeCount && <Pill label={getCountText()} size="xs" variant="filled" color="gray" />}
                <Checkbox
                    isChecked={isSelected}
                    isIntermediate={isIndeterminate}
                    onCheckboxChange={() => updateFilterValues()}
                    dataTestId={`filter-option-${label}`}
                    size="xs"
                />
            </FilterOptionWrapper>
            {areChildrenVisible && (
                <>
                    {nestedOptions?.map((option) => (
                        <FilterOption
                            key={option.value}
                            filterOption={option}
                            selectedFilterOptions={selectedFilterOptions}
                            setSelectedFilterOptions={setSelectedFilterOptions}
                            addPadding
                        />
                    ))}
                </>
            )}
        </>
    );
}
