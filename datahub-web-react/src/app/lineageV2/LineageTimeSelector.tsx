import { CalendarOutlined, CaretDownOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Button, Space, Typography } from 'antd';
import dayjs from '@utils/dayjs';
import type { Dayjs } from '@utils/dayjs';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import DatePicker from '@utils/DayjsDatePicker';

const { RangePicker } = DatePicker;

export type Datetime = Dayjs | null;

const ConfirmButtonWrapper = styled.div`
    position: absolute;
    left: 0;
    bottom: 0;
    width: 100%;
`;

const ConfirmButton = styled(Button)`
    border-radius: 15px;
    border: 1px solid ${(props) => props.theme.colors.text};

    position: absolute;
    right: 10px;
    bottom: 13px;
    text-align: right;

    :hover {
        border-color: ${(props) => props.theme.colors.hyperlinks};
        color: ${(props) => props.theme.colors.hyperlinks};
    }
`;

export type Props = {
    onChange: (start: Datetime, end: Datetime) => void;
    startTimeMillis?: number;
    endTimeMillis?: number;
};

export default function LineageTimeSelector({ onChange, startTimeMillis, endTimeMillis }: Props) {
    const [startDate, setStartDate] = useState<Datetime>(startTimeMillis ? dayjs(startTimeMillis) : null);
    const [endDate, setEndDate] = useState<Datetime>(endTimeMillis ? dayjs(endTimeMillis) : null);
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const ref = useRef<any>(null);

    useEffect(() => {
        setStartDate(startTimeMillis ? dayjs(startTimeMillis) : null);
    }, [startTimeMillis]);

    useEffect(() => {
        setEndDate(endTimeMillis ? dayjs(endTimeMillis) : null);
    }, [endTimeMillis]);

    const handleOpenChange = useCallback(
        (open: boolean) => {
            setIsOpen(open);
            if (!open) {
                ref.current?.blur();
                onChange(startDate, endDate);
            }
        },
        [onChange, startDate, endDate],
    );

    const handleRangeChange = useCallback((dates: [Datetime, Datetime] | null) => {
        const [start, end] = dates || [null, null];

        setStartDate(start?.startOf('day') ?? null);
        setEndDate(end?.endOf('day') ?? null);
    }, []);

    const showText = !isOpen && (startDate === null || endDate === null);

    const [ranges] = useState<Array<[Datetime, Datetime]>>([
        [dayjs().subtract(7, 'days'), null],
        [dayjs().subtract(14, 'days'), null],
        [dayjs().subtract(28, 'days'), null],
        [null, null],
    ]);

    return (
        <>
            {showText ? ( // Conditionally render All Time selection
                <Tooltip title="Filter lineage edges by observed date" placement="topLeft" showArrow={false}>
                    <Button type="text" onClick={() => handleOpenChange(true)}>
                        <CalendarOutlined style={{ marginRight: '4px' }} />
                        <Typography.Text>
                            <b>{getTimeRangeDescription(startDate, endDate)}</b>
                        </Typography.Text>
                        <CaretDownOutlined style={{ fontSize: '10px' }} />
                    </Button>
                </Tooltip>
            ) : (
                <Space direction="vertical" size={12}>
                    <RangePicker
                        ref={ref}
                        open={isOpen}
                        allowClear
                        allowEmpty={[true, true]}
                        bordered={false}
                        value={[startDate, endDate]}
                        disabledDate={(current: any) => {
                            return current && current > dayjs().endOf('day');
                        }}
                        renderExtraFooter={() => (
                            <ConfirmButtonWrapper>
                                <ConfirmButton type="text" onClick={() => handleOpenChange(false)}>
                                    <b>Confirm</b>
                                </ConfirmButton>
                            </ConfirmButtonWrapper>
                        )}
                        format="ll"
                        ranges={Object.fromEntries(
                            ranges.map(([start, end]) => [getTimeRangeDescription(start, end), [start, end]]),
                        )}
                        onChange={handleRangeChange}
                        onOpenChange={handleOpenChange}
                        onCalendarChange={() => handleOpenChange(true)}
                    />
                </Space>
            )}
        </>
    );
}

function getTimeRangeDescription(startDate: Dayjs | null, endDate: Dayjs | null): string {
    if (!startDate && !endDate) {
        return 'All Time';
    }

    if (!startDate && endDate) {
        return `Until ${endDate.format('ll')}`;
    }

    if (startDate && !endDate) {
        const dayDiff = dayjs().diff(startDate, 'days');
        if (dayDiff <= 30) {
            return `Last ${dayDiff} days`;
        }
        return `From ${startDate.format('ll')}`;
    }

    return 'Unknown time range';
}
