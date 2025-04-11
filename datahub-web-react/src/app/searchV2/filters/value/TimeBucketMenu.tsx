import { ANTD_GRAY } from '@app/entity/shared/constants';
import OptionsDropdownMenu from '@app/searchV2/filters/OptionsDropdownMenu';
import { FilterValue, TimeBucketFilterField } from '@app/searchV2/filters/types';
import { OptionMenu } from '@app/searchV2/filters/value/styledComponents';
import type { ItemType } from 'antd/lib/menu/hooks/useItems';
import moment from 'moment';
import React, { useMemo } from 'react';
import styled from 'styled-components';

const FilterOptionWrapper = styled.div`
    display: flex;
    align-items: center;

    margin: 0 4px;
    padding: 10px;

    border-radius: 8px;

    font-size: 14px;

    &:hover {
        background-color: ${ANTD_GRAY[3]};
    }
`;

interface Props {
    field: TimeBucketFilterField;
    values: FilterValue[];
    onChangeValues: (newValues: FilterValue[]) => void;
    onApply: () => void;
    type?: 'card' | 'default';
    className?: string;
}

export default function TimeBucketMenu({ field, values, type = 'card', onChangeValues, onApply, className }: Props) {
    const filterMenuOptions = useMemo(
        () =>
            field.options.map(({ label, startOffsetMillis }): ItemType => {
                const timestamp = moment()
                    .subtract(startOffsetMillis, 'milliseconds')
                    .set({ hour: 0, minute: 0, second: 0, millisecond: 0 })
                    .valueOf()
                    .toString();
                return {
                    key: timestamp,
                    label: <FilterOptionWrapper>{label}</FilterOptionWrapper>,
                    onClick: () => onChangeValues([{ value: timestamp, entity: null }]),
                };
            }),
        [field.options, onChangeValues],
    );

    const selectedKey = useMemo(
        () => filterMenuOptions.find((option) => values.length && option?.key === values[0].value)?.key,
        [filterMenuOptions, values],
    );

    return (
        <OptionsDropdownMenu
            menu={
                <OptionMenu
                    items={filterMenuOptions}
                    selectedKeys={selectedKey ? [selectedKey.toString()] : undefined}
                />
            }
            updateFilters={onApply}
            showSearchBar={false}
            type={type}
            className={className}
        />
    );
}
