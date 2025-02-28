/* eslint-disable import/no-cycle */
import { CloseOutlined } from '@ant-design/icons';
import { Button, DatePicker } from 'antd';
import moment from 'moment';
import React from 'react';
import styled from 'styled-components/macro';
import { SEARCH_COLORS } from '../../entityV2/shared/constants';
import OperatorSelector from './OperatorSelector';
import { operatorRequiresValues } from './operator/operator';
import { FilterOperatorType, FilterPredicate, FilterValue } from './types';
import ValueSelector from './value/ValueSelector';
import ValueName from './value/ValueName';
import { getIsDateRangeFilter, useFilterDisplayName } from './utils';

const Values = styled.div`
    border: 1.5px solid transparent;
    border-radius: 6px;
    padding: 2px;

    :hover {
        cursor: pointer;
        border: 1.5px solid ${SEARCH_COLORS.TITLE_PURPLE};
        background-color: ${SEARCH_COLORS.TITLE_PURPLE};
        color: #fff;
    }
`;

const Value = styled.span``;

const Container = styled.div<{ $isCompact?: boolean }>`
    border-radius: 4px;
    padding: 4px 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 14px;
    margin-right: 8px;
    background-color: ${SEARCH_COLORS.BACKGROUND_PURPLE};

    ${(props) =>
        props.$isCompact &&
        `
        font-size: 12px;
        padding: 0 6px;
    `}
`;

const Icon = styled.div`
    margin-right: 4px;
    display: flex;
    align-items: center;
`;

const FilterName = styled.div`
    margin-right: 2px;
    color: ${SEARCH_COLORS.LINK_BLUE};
    display: flex;
    align-items: center;
`;

const RemoveButton = styled(Button)`
    padding: 0px;
    margin: 0px;
    width: 20px;
    height: 20px;
    margin-left: 0px;
`;

interface SelectedFilterProps {
    predicate: FilterPredicate;
    onChangeOperator: (operator: FilterOperatorType) => void;
    onChangeValues: (newValues: FilterValue[]) => void;
    onRemoveFilter: () => void;
    isCompact?: boolean;
}

export default function SelectedFilter({
    predicate,
    onChangeOperator,
    onChangeValues,
    onRemoveFilter,
    isCompact,
}: SelectedFilterProps) {
    moment.tz.setDefault('GMT');
    const { field, operator, values, defaultValueOptions } = predicate;
    const showValueSelector = operatorRequiresValues(predicate.operator) || false;
    const displayName = useFilterDisplayName(predicate.field);
    const isDateRangeFilter = getIsDateRangeFilter(predicate.field);

    const useDatePicker = field.useDatePicker || isDateRangeFilter;

    return (
        <Container
            $isCompact={isCompact}
            data-testid={`active-filter-${field.field}`}
            key={`${field.field}-${operator}-${values.map((value) => value.value).join('-')}`}
        >
            <FilterName>
                {(field.icon && <Icon>{field.icon}</Icon>) || null}
                {displayName || field.field}
            </FilterName>
            <OperatorSelector predicate={predicate} onChangeOperator={onChangeOperator} />
            {showValueSelector && useDatePicker && (
                <DatePicker
                    defaultValue={moment(Number(values[0].value))}
                    disabledDate={isDateRangeFilter ? undefined : (current) => current > moment().startOf('day')}
                    format="ll"
                    showToday={false}
                    allowClear={false}
                    onChange={(v) => onChangeValues(v ? [{ value: v.valueOf().toString(), entity: null }] : [])}
                />
            )}
            {showValueSelector && !useDatePicker && (
                <ValueSelector
                    field={field}
                    values={values}
                    defaultOptions={defaultValueOptions}
                    onChangeValues={onChangeValues}
                >
                    <Values>
                        {values.map((value, index) => (
                            <Value data-testid={`active-filter-value-${field.field}-${value.value}`}>
                                <ValueName field={field} value={value} />
                                {index < values.length - 1 ? ', ' : ''}
                            </Value>
                        ))}
                    </Values>
                </ValueSelector>
            )}
            <RemoveButton
                type="text"
                icon={<CloseOutlined style={{ fontSize: 12 }} />}
                onClick={onRemoveFilter}
                data-testid={`remove-filter-${field.field}`}
            />
        </Container>
    );
}
