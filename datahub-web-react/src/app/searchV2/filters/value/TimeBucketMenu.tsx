import React, { useMemo } from 'react';
import styled from 'styled-components';

import OptionsDropdownMenu from '@app/searchV2/filters/OptionsDropdownMenu';
import { FilterValue, TimeBucketFilterField } from '@app/searchV2/filters/types';
import dayjs from '@utils/dayjs';

interface TimeBucketOptionType {
    key: string;
    label: string;
    timestamp: string;
}

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
    isRenderedInSubMenu?: boolean;
}

export default function TimeBucketMenu({ field, values, onChangeValues, className, isRenderedInSubMenu }: Props) {
    const options = useMemo(
        () =>
            field.options.map(({ label, startOffsetMillis }): TimeBucketOptionType => {
                const timestamp = dayjs()
                    .subtract(startOffsetMillis, 'milliseconds')
                    .startOf('day')
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
            isRenderedInSubMenu={isRenderedInSubMenu}
        />
    );
}
