import { CloseCircleOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getFilterEntity, getFilterIconAndLabel } from './utils';

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

const Label = styled(Typography.Text)`
    max-width: 125px;
`;

const IconSpacer = styled.span`
    width: 4px;
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
    filterField: string;
    filterValue: string;
    availableFilters: FacetMetadata[] | null;
}

function ActiveFilter({ filterField, filterValue, availableFilters }: ActiveFilterProps) {
    const entityRegistry = useEntityRegistry();
    const filterEntity = getFilterEntity(filterField, filterValue, availableFilters);
    const { icon, label } = getFilterIconAndLabel(filterField, filterValue, entityRegistry, filterEntity);

    return (
        <ActiveFilterWrapper>
            {icon}
            {icon && <IconSpacer />}
            <Label ellipsis={{ tooltip: label }} style={{ maxWidth: 150 }}>
                {label}
            </Label>
            <StyledButton icon={<CloseCircleOutlined />} />
        </ActiveFilterWrapper>
    );
}

interface Props {
    filter: FacetFilterInput;
    availableFilters: FacetMetadata[] | null;
}

export default function ActiveFilterContainer({ filter, availableFilters }: Props) {
    return (
        <>
            {filter.values?.map((value) => (
                <ActiveFilter filterField={filter.field} filterValue={value} availableFilters={availableFilters} />
            ))}
        </>
    );
}
