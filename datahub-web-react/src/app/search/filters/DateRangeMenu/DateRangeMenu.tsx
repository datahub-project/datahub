import dayjs from 'dayjs';
import React, { useCallback, useRef, useState } from 'react';
import styled from 'styled-components';

import useDateRangeFilterValues, { Datetime } from '@app/search/filters/DateRangeMenu/useDateRangeFilterValues';
import { useFilterDisplayName } from '@app/search/filters/utils';
import { Text } from '@src/alchemy-components';
import { FacetFilterInput, FacetMetadata, FilterOperator } from '@src/types.generated';
import DatePicker from '@utils/DayjsDatePicker';

const { RangePicker } = DatePicker;

const Container = styled.div`
    padding: 16px;
    background-color: #ffffff;
    box-shadow:
        0 3px 6px -4px rgba(0, 0, 0, 0.12),
        0 6px 16px 0 rgba(0, 0, 0, 0.08),
        0 9px 28px 8px rgba(0, 0, 0, 0.05);
    border-radius: 8px;
    min-width: 225px;
`;

interface Props {
    field: FacetMetadata;
    manuallyUpdateFilters: (newValues: FacetFilterInput[]) => void;
}

export default function DateRangeMenu({ field, manuallyUpdateFilters }: Props) {
    const displayName = useFilterDisplayName(field);
    dayjs.tz.setDefault('GMT');

    const [startDate, setStartDate] = useState<Datetime>(null);
    const [endDate, setEndDate] = useState<Datetime>(null);
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const ref = useRef<any>(null);

    useDateRangeFilterValues({ filterField: field.field, setStartDate, setEndDate });

    const handleOpenChange = useCallback(
        (open: boolean) => {
            setIsOpen(open);
            if (!open) {
                ref.current?.blur();
                if (startDate && endDate) {
                    manuallyUpdateFilters([
                        {
                            field: field.field,
                            values: [startDate.valueOf().toString()],
                            condition: FilterOperator.GreaterThan,
                        },
                        {
                            field: field.field,
                            values: [endDate.valueOf().toString()],
                            condition: FilterOperator.LessThan,
                        },
                    ]);
                }
            }
        },
        [startDate, endDate, field.field, manuallyUpdateFilters],
    );

    const handleRangeChange = useCallback((dates: [Datetime, Datetime] | null) => {
        const [start, end] = dates || [null, null];

        start?.startOf('day');
        end?.endOf('day');

        setStartDate(start);
        setEndDate(end);
    }, []);

    return (
        <Container>
            <Text weight="bold">Filter by {displayName}</Text>
            <RangePicker
                ref={ref}
                open={isOpen}
                allowClear
                allowEmpty={[true, true]}
                bordered={false}
                value={[startDate, endDate]}
                format="ll"
                onChange={handleRangeChange}
                onOpenChange={handleOpenChange}
                onCalendarChange={() => handleOpenChange(true)}
                style={{ paddingLeft: 0, paddingRight: 0 }}
            />
        </Container>
    );
}
