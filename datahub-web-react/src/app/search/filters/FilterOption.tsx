import { Checkbox } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FilterOptionType } from './types';
import { EntityType, GlossaryNode, GlossaryTerm, Tag } from '../../../types.generated';
import { generateColor } from '../../entity/shared/components/styled/StyledTag';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PLATFORM_FILTER_NAME, TAGS_FILTER_NAME, TYPE_NAMES_FILTER_NAME } from '../utils/constants';
import { IconSpacer, Label } from './ActiveFilter';
import { isFilterOptionSelected, getFilterIconAndLabel } from './utils';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import ParentNodes from './ParentNodes';

const FilterOptionWrapper = styled.div<{ centerAlign?: boolean }>`
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
    filterOption: FilterOptionType;
    selectedFilterOptions: FilterOptionType[];
    setSelectedFilterOptions: (filterValues: FilterOptionType[]) => void;
}

export default function FilterOption({ filterOption, selectedFilterOptions, setSelectedFilterOptions }: Props) {
    const { field, value, count, entity } = filterOption;
    const entityRegistry = useEntityRegistry();
    const { icon, label } = getFilterIconAndLabel(field, value, entityRegistry, entity || null, 14);
    const shouldShowIcon = field === PLATFORM_FILTER_NAME && icon !== null;
    const shouldShowTagColor = field === TAGS_FILTER_NAME && entity?.type === EntityType.Tag;
    const isSubTypeFilter = field === TYPE_NAMES_FILTER_NAME;
    const isGlossaryTerm = entity?.type === EntityType.GlossaryTerm;
    const parentNodes: GlossaryNode[] = isGlossaryTerm ? (entity as GlossaryTerm).parentNodes?.nodes || [] : [];

    function updateFilterValues() {
        if (isFilterOptionSelected(selectedFilterOptions, value)) {
            setSelectedFilterOptions(selectedFilterOptions.filter((option) => option.value !== value));
        } else {
            setSelectedFilterOptions([...selectedFilterOptions, filterOption]);
        }
    }

    return (
        <FilterOptionWrapper centerAlign={parentNodes.length > 0}>
            <StyledCheckbox checked={isFilterOptionSelected(selectedFilterOptions, value)} onClick={updateFilterValues}>
                {isGlossaryTerm && <ParentNodes glossaryTerm={entity as GlossaryTerm} />}
                <CheckboxContent>
                    {shouldShowIcon && <>{icon}</>}
                    {shouldShowTagColor && (
                        <TagColor color={(entity as Tag).properties?.colorHex || ''} colorHash={entity?.urn} />
                    )}
                    {(shouldShowIcon || shouldShowTagColor) && <IconSpacer />}
                    <LabelCountWrapper>
                        <Label ellipsis={{ tooltip: label }} style={{ maxWidth: 150 }}>
                            {isSubTypeFilter ? capitalizeFirstLetterOnly(label as string) : label}
                        </Label>
                        <CountText>{count}</CountText>
                    </LabelCountWrapper>
                </CheckboxContent>
            </StyledCheckbox>
        </FilterOptionWrapper>
    );
}
