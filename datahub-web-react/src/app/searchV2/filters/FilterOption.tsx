import { CaretUpOutlined } from '@ant-design/icons';
import { Button, Checkbox } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import colors from '@components/theme/foundations/colors';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { generateColor } from '@app/entityV2/shared/components/styled/StyledTag';
import ParentEntities from '@app/searchV2/filters/ParentEntities';
import { Label } from '@app/searchV2/filters/styledComponents';
import { FilterOptionType } from '@app/searchV2/filters/types';
import {
    FilterEntityIcon,
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
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType } from '@types';

const FilterOptionWrapper = styled.div<{ addPadding?: boolean }>`
    display: flex;
    align-items: center;
    border-radius: 8px;
    margin: 0px 4px;

    label {
        padding: 12px;
        width: 100%;
        height: 100%;
        display: flex;
        align-items: center;
    }

    ${(props) => props.addPadding && 'padding-left: 16px;'}
    &:hover {
        background-color: ${ANTD_GRAY[3]};
    }
`;

const StyledCheckbox = styled(Checkbox)`
    font-size: 14px;

    .ant-checkbox-inner {
        border-color: ${ANTD_GRAY[7]};
    }

    .ant-checkbox-checked {
        .ant-checkbox-inner {
            border-color: ${(props) => props.theme.styles['primary-color']};
        }
    }
`;

const CheckboxContent = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

export const TagColor = styled.span<{ color: string; colorHash?: string | null }>`
    height: 8px;
    width: 8px;
    border-radius: 50%;
    background-color: ${(props) => (props.color ? props.color : generateColor.hex(props.colorHash))};
    margin-right: 3px;
`;

const CountText = styled.span`
    font-size: 12px;
    margin-left: 6px;
    color: ${ANTD_GRAY[8]};
`;

const LabelCountWrapper = styled.div`
    display: flex;
    align-items: baseline;
`;

const ArrowButton = styled(Button)<{ isOpen: boolean }>`
    margin-left: 4px;
    background-color: transparent;
    height: 24px;

    svg {
        height: 12px;
        width: 12px;
    }

    &:hover,
    &:focus,
    &:active {
        background-color: transparent;
    }

    ${(props) =>
        props.isOpen &&
        `
        transform: rotate(180deg);
    `}
`;

const ParentWrapper = styled.div`
    max-width: 220px;
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
    const { icon, label } = getFilterIconAndLabel(
        field,
        value,
        entityRegistry,
        entity || null,
        14,
        filterOption.displayName,
    );
    const showParentEntityPath = field === DOMAINS_FILTER_NAME && entity?.type === EntityType.Domain;
    const isSubTypeFilter = field === TYPE_NAMES_FILTER_NAME;
    const parentEntities: Entity[] = getParentEntities(entity as Entity) || [];

    const getCountText = () => {
        // only entity type filters return 10,000 max aggs
        return count === MAX_COUNT_VAL && field === ENTITY_SUB_TYPE_FILTER_NAME ? '10k+' : formatNumber(count);
    };

    function updateFilterValues() {
        if (isFilterOptionSelected(selectedFilterOptions, value)) {
            setSelectedFilterOptions(selectedFilterOptions.filter((option) => option.value !== value));
        } else {
            // if selecting parent filter, remove nested filter values
            const filteredSelectedOptions = selectedFilterOptions.filter(
                (o) => !nestedOptions?.some((nestedOption) => nestedOption.value === o.value),
            );
            setSelectedFilterOptions([...filteredSelectedOptions, filterOption]);
        }
    }

    return (
        <>
            <FilterOptionWrapper addPadding={addPadding}>
                <StyledCheckbox
                    checked={isFilterOptionSelected(selectedFilterOptions, value)}
                    // show indeterminate if a nested option is selected
                    indeterminate={isAnyOptionSelected(
                        selectedFilterOptions,
                        nestedOptions?.map((o) => o.value),
                    )}
                    onClick={updateFilterValues}
                    data-testid={`filter-option-${label}`}
                >
                    <CheckboxContent>
                        <FilterEntityIcon field={field} entity={entity} icon={icon} />
                        <LabelWrapper className="test-label">
                            {!showParentEntityPath && parentEntities.length > 0 && (
                                <ParentWrapper>
                                    <ParentEntities parentEntities={parentEntities} />
                                </ParentWrapper>
                            )}
                            <LabelCountWrapper>
                                <Label
                                    ellipsis={{
                                        tooltip: {
                                            title: label,
                                            showArrow: false,
                                            color: 'white',
                                            overlayInnerStyle: { color: colors.gray[1700] },
                                        },
                                    }}
                                    style={{ maxWidth: 150 }}
                                >
                                    {isSubTypeFilter ? capitalizeFirstLetterOnly(label as string) : label}
                                </Label>
                                {includeCount && <CountText>{getCountText()}</CountText>}
                                {nestedOptions && nestedOptions.length > 0 && (
                                    <ArrowButton
                                        icon={<CaretUpOutlined />}
                                        type="text"
                                        onClick={() => setAreChildrenVisible(!areChildrenVisible)}
                                        isOpen={areChildrenVisible}
                                    />
                                )}
                            </LabelCountWrapper>
                        </LabelWrapper>
                    </CheckboxContent>
                </StyledCheckbox>
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
