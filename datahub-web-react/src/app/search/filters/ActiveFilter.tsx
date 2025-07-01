import { CloseCircleOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { useFilterRendererRegistry } from '@app/search/filters/render/useFilterRenderer';
import { IconSpacer, Label } from '@app/search/filters/styledComponents';
import useGetBrowseV2LabelOverride from '@app/search/filters/useGetBrowseV2LabelOverride';
import { getFilterEntity, getFilterIconAndLabel, getNewFilters } from '@app/search/filters/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { FacetFilterInput, FacetMetadata } from '@types';

const ActiveFilterWrapper = styled.div`
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 4px;
    padding: 2px 8px;
    display: flex;
    align-items: center;
    font-size: 14px;
    height: 24px;
    margin: 8px 8px 0 0;
`;

const StyledButton = styled(Button)`
    border: none;
    box-shadow: none;
    padding: 0;
    margin-left: 6px;
    height: 16px;
    width: 11px;
`;

interface ActiveFilterProps {
    filterFacet: FacetFilterInput;
    filterValue: string;
    availableFilters: FacetMetadata[] | null;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

function ActiveFilter({
    filterFacet,
    filterValue,
    availableFilters,
    activeFilters,
    onChangeFilters,
}: ActiveFilterProps) {
    const entityRegistry = useEntityRegistry();
    const filterEntity = getFilterEntity(filterFacet.field, filterValue, availableFilters);
    const filterLabelOverride = useGetBrowseV2LabelOverride(filterFacet.field, filterValue, entityRegistry);
    const filterRenderer = useFilterRendererRegistry();
    const facetEntity = availableFilters?.find((f) => f.field === filterFacet.field)?.entity;

    const { icon, label } = !filterRenderer.hasRenderer(filterFacet.field)
        ? getFilterIconAndLabel(
              filterFacet.field,
              filterValue,
              entityRegistry,
              filterEntity,
              12,
              filterLabelOverride,
              facetEntity,
          )
        : {
              icon: filterRenderer.getIcon(filterFacet.field),
              label: filterRenderer.getValueLabel(filterFacet.field, filterValue),
          };

    function removeFilter() {
        const newFilterValues = filterFacet.values?.filter((value) => value !== filterValue) || [];
        onChangeFilters(getNewFilters(filterFacet.field, activeFilters, newFilterValues));
    }

    return (
        <ActiveFilterWrapper data-testid={`active-filter-${label}`}>
            {icon}
            {icon && <IconSpacer />}
            <Label ellipsis={{ tooltip: label }} style={{ maxWidth: 150 }}>
                {label}
            </Label>
            <StyledButton
                icon={<CloseCircleOutlined />}
                onClick={removeFilter}
                data-testid={`remove-filter-${label}`}
            />
        </ActiveFilterWrapper>
    );
}

interface Props {
    filter: FacetFilterInput;
    availableFilters: FacetMetadata[] | null;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function ActiveFilterContainer({ filter, availableFilters, activeFilters, onChangeFilters }: Props) {
    return (
        <>
            {filter.values?.map((value) => (
                <ActiveFilter
                    key={value}
                    filterFacet={filter}
                    filterValue={value}
                    availableFilters={availableFilters}
                    activeFilters={activeFilters}
                    onChangeFilters={onChangeFilters}
                />
            ))}
        </>
    );
}
