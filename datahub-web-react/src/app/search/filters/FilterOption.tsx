import { Checkbox } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FilterFields } from './types';
import { EntityType, Tag } from '../../../types.generated';
import { generateColor } from '../../entity/shared/components/styled/StyledTag';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PLATFORM_FILTER_NAME, TAGS_FILTER_NAME } from '../utils/constants';
import { IconSpacer, Label } from './ActiveFilter';
import { isFilterOptionSelected, getFilterIconAndLabel } from './utils';

const FilterOptionWrapper = styled.div`
    label {
        padding: 5px 12px;
        width: 100%;
        height: 100%;
    }
`;

const StyledCheckbox = styled(Checkbox)`
    font-size: 14px;
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

interface Props {
    filterFields: FilterFields;
    selectedFilterValues: string[];
    setSelectedFilterValues: (filterValues: string[]) => void;
}

export default function FilterOption({ filterFields, selectedFilterValues, setSelectedFilterValues }: Props) {
    const { field, value, count, entity } = filterFields;
    const entityRegistry = useEntityRegistry();
    const { icon, label } = getFilterIconAndLabel(field, value, entityRegistry, entity || null, 14);
    const shouldShowIcon = field === PLATFORM_FILTER_NAME && icon !== null;
    const shouldShowTagColor = field === TAGS_FILTER_NAME && entity?.type === EntityType.Tag;

    function updateFilterValues() {
        if (isFilterOptionSelected(selectedFilterValues, value)) {
            setSelectedFilterValues(selectedFilterValues.filter((v) => v !== value));
        } else {
            setSelectedFilterValues([...selectedFilterValues, value]);
        }
    }

    return (
        <FilterOptionWrapper>
            <StyledCheckbox checked={isFilterOptionSelected(selectedFilterValues, value)} onClick={updateFilterValues}>
                <CheckboxContent>
                    {shouldShowIcon && <>{icon}</>}
                    {shouldShowTagColor && (
                        <TagColor color={(entity as Tag).properties?.colorHex || ''} colorHash={entity?.urn} />
                    )}
                    {(shouldShowIcon || shouldShowTagColor) && <IconSpacer />}
                    <LabelCountWrapper>
                        <Label ellipsis={{ tooltip: label }} style={{ maxWidth: 150 }}>
                            {label}
                        </Label>
                        <CountText>{count}</CountText>
                    </LabelCountWrapper>
                </CheckboxContent>
            </StyledCheckbox>
        </FilterOptionWrapper>
    );
}
