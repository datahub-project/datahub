import moment from 'moment';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import OptionsDropdownMenu from '@app/searchV2/filters/OptionsDropdownMenu';
import { FilterValue, TimeBucketFilterField } from '@app/searchV2/filters/types';

const TimeBucketOption = styled.div<{ $isSelected: boolean }>`
    display: flex;
    align-items: center;
    padding: 8px 4px;
    border-radius: 8px;
    font-size: 14px;
    cursor: pointer;
    color: ${(props) => props.theme.colors.text};
    background-color: ${(props) => (props.$isSelected ? props.theme.colors.bgSelectedSubtle : 'transparent')};

    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
`;

interface Props {
    field: TimeBucketFilterField;
    values: FilterValue[];
    onChangeValues: (newValues: FilterValue[]) => void;
    className?: string;
}

export default function TimeBucketMenu({ field, values, onChangeValues, className }: Props) {
    const options = useMemo(
        () =>
            field.options.map(({ label, startOffsetMillis }) => {
                const timestamp = moment()
                    .subtract(startOffsetMillis, 'milliseconds')
                    .set({ hour: 0, minute: 0, second: 0, millisecond: 0 })
                    .valueOf()
                    .toString();
                return { key: timestamp, label, timestamp };
            }),
        [field.options],
    );

    const selectedKey = useMemo(
        () => options.find((option) => values.length && option.key === values[0].value)?.key,
        [options, values],
    );

    return (
        <OptionsDropdownMenu
            menu={
                <div>
                    {options.map((option) => (
                        <TimeBucketOption
                            key={option.key}
                            $isSelected={option.key === selectedKey}
                            onClick={() => onChangeValues([{ value: option.timestamp, entity: null }])}
                        >
                            {option.label}
                        </TimeBucketOption>
                    ))}
                </div>
            }
            showSearchBar={false}
            className={className}
        />
    );
}
