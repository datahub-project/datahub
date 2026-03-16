import dayjs from 'dayjs';
import React, { useCallback, useRef, useState } from 'react';
import styled from 'styled-components';

import { FilterField } from '@app/searchV2/filters/types';
import { useFilterDisplayName } from '@app/searchV2/filters/utils';
import useDateRangeFilterValues from '@app/searchV2/filters/value/useDateRangeFilterValues';
import { Text } from '@src/alchemy-components';
import { Datetime } from '@src/app/lineageV2/LineageTimeSelector';
import { FacetFilterInput, FacetMetadata, FilterOperator } from '@src/types.generated';
import DatePicker from '@utils/DayjsDatePicker';

const { RangePicker } = DatePicker;

const Container = styled.div`
    padding: 16px;
    background-color: ${(props) => props.theme.colors.bg};
    box-shadow: ${(props) => props.theme.colors.shadowLg};
    border-radius: 8px;
    min-width: 225px;
`;

interface Props {
    field: FilterField | FacetMetadata;
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
