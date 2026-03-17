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
    background-color: ${(props) => props.theme.colors.bg};
    box-shadow: ${(props) => props.theme.colors.shadowLg};
    border-radius: 8px;
    min-width: 225px;
`;

interface Props {
    field: FacetMetadata;
    manuallyUpdateFilters: (newValues: FacetFilterInput[]) => void;
}

export default function DateRangeMenu({ field, manuallyUpdateFilters }: Props) {
    const displayName = useFilterDisplayName(field);

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

        setStartDate(start?.startOf('day') ?? null);
        setEndDate(end?.endOf('day') ?? null);
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
