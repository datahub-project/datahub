import { CaretUpOutlined } from '@ant-design/icons';
import { Button, Checkbox } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FilterOptionType } from './types';
import { Entity, EntityType, Tag } from '../../../types.generated';
import { generateColor } from '../../entity/shared/components/styled/StyledTag';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import {
    CONTAINER_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    MAX_COUNT_VAL,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
} from '../utils/constants';
import { IconSpacer, Label } from './styledComponents';
import { isFilterOptionSelected, getFilterIconAndLabel, isAnyOptionSelected, getParentEntities } from './utils';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import ParentEntities from './ParentEntities';
import { formatNumber } from '../../shared/formatNumber';

const FilterOptionWrapper = styled.div<{ centerAlign?: boolean; addPadding?: boolean }>`
    display: flex;
    align-items: center;

    label {
        padding: 5px 12px;
        width: 100%;
        height: 100%;
        ${(props) =>
            props.centerAlign &&
            `
            display: flex;
            align-items: center;
        `}
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
`;

const TagColor = styled.span<{ color: string; colorHash?: string | null }>`
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

const LabelCountWrapper = styled.span`
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

interface Props {
    filterOption: FilterOptionType;
    selectedFilterOptions: FilterOptionType[];
    setSelectedFilterOptions: (filterValues: FilterOptionType[]) => void;
    nestedOptions?: FilterOptionType[];
    addPadding?: boolean;
}

export default function FilterOption({
    filterOption,
    selectedFilterOptions,
    setSelectedFilterOptions,
    nestedOptions,
    addPadding,
}: Props) {
    const [areChildrenVisible, setAreChildrenVisible] = useState(true);
    const { field, value, count, entity, displayName } = filterOption;
    const entityRegistry = useEntityRegistry();
    const { icon, label } = getFilterIconAndLabel(field, value, entityRegistry, entity || null, 14);
    const finalLabel = displayName || label;
    const shouldShowIcon = (field === PLATFORM_FILTER_NAME || field === CONTAINER_FILTER_NAME) && icon !== null;
    const shouldShowTagColor = field === TAGS_FILTER_NAME && entity?.type === EntityType.Tag;
    const isSubTypeFilter = field === TYPE_NAMES_FILTER_NAME;
    const parentEntities: Entity[] = getParentEntities(entity as Entity) || [];
    // only entity type filters return 10,000 max aggs
    const countText = count === MAX_COUNT_VAL && field === ENTITY_SUB_TYPE_FILTER_NAME ? '10k+' : formatNumber(count);

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
            <FilterOptionWrapper centerAlign={parentEntities.length > 0} addPadding={addPadding}>
                <StyledCheckbox
                    checked={isFilterOptionSelected(selectedFilterOptions, value)}
                    // show indeterminate if a nested option is selected
                    indeterminate={isAnyOptionSelected(
                        selectedFilterOptions,
                        nestedOptions?.map((o) => o.value),
                    )}
                    onClick={updateFilterValues}
                    data-testid={`filter-option-${finalLabel}`}
                >
                    {parentEntities.length > 0 && (
                        <ParentWrapper>
                            <ParentEntities parentEntities={parentEntities} />
                        </ParentWrapper>
                    )}
                    <CheckboxContent>
                        {shouldShowIcon && <>{icon}</>}
                        {shouldShowTagColor && (
                            <TagColor color={(entity as Tag).properties?.colorHex || ''} colorHash={entity?.urn} />
                        )}
                        {(shouldShowIcon || shouldShowTagColor) && <IconSpacer />}
                        <LabelCountWrapper>
                            <Label ellipsis={{ tooltip: finalLabel }} style={{ maxWidth: 150 }}>
                                {isSubTypeFilter ? capitalizeFirstLetterOnly(finalLabel as string) : finalLabel}
                            </Label>
                            <CountText>{countText}</CountText>
                            {nestedOptions && nestedOptions.length > 0 && (
                                <ArrowButton
                                    icon={<CaretUpOutlined />}
                                    type="text"
                                    onClick={() => setAreChildrenVisible(!areChildrenVisible)}
                                    isOpen={areChildrenVisible}
                                />
                            )}
                        </LabelCountWrapper>
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
