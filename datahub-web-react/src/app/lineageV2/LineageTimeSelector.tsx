import React, { useCallback, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';
import moment from 'moment';
import { DatePicker, Space, Button, Typography } from 'antd';
import { Tooltip } from '@components';
import { CalendarOutlined, CaretDownOutlined } from '@ant-design/icons';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

const { RangePicker } = DatePicker;

export type Datetime = moment.Moment | null;

const ConfirmButtonWrapper = styled.div`
    position: absolute;
    left: 0;
    bottom: 0;
    width: 100%;
`;

const ConfirmButton = styled(Button)`
    border-radius: 15px;
    border: 1px solid ${REDESIGN_COLORS.BLACK};

    position: absolute;
    right: 10px;
    bottom: 13px;
    text-align: right;

    :hover {
        border-color: ${REDESIGN_COLORS.BLUE};
        color: ${REDESIGN_COLORS.BLUE};
    }
`;

export type Props = {
    onChange: (start: Datetime, end: Datetime) => void;
    startTimeMillis?: number;
    endTimeMillis?: number;
};

export default function LineageTimeSelector({ onChange, startTimeMillis, endTimeMillis }: Props) {
    const [startDate, setStartDate] = useState<Datetime>(startTimeMillis ? moment(startTimeMillis) : null);
    const [endDate, setEndDate] = useState<Datetime>(endTimeMillis ? moment(endTimeMillis) : null);
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const ref = useRef<any>(null);

    useEffect(() => {
        setStartDate(startTimeMillis ? moment(startTimeMillis) : null);
    }, [startTimeMillis]);

    useEffect(() => {
        setEndDate(endTimeMillis ? moment(endTimeMillis) : null);
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

        start?.set({ hour: 0, minute: 0, second: 0, millisecond: 0 });
        end?.set({ hour: 23, minute: 59, second: 59, millisecond: 999 });

        setStartDate(start);
        setEndDate(end);
    }, []);

    const showText = !isOpen && (startDate === null || endDate === null);

    const [ranges] = useState<Array<[Datetime, Datetime]>>([
        [moment().subtract(7, 'days'), null],
        [moment().subtract(14, 'days'), null],
        [moment().subtract(28, 'days'), null],
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
                            return current && current > moment().endOf('day');
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

function getTimeRangeDescription(startDate: moment.Moment | null, endDate: moment.Moment | null): string {
    if (!startDate && !endDate) {
        return 'All Time';
    }

    if (!startDate && endDate) {
        return `Until ${endDate.format('ll')}`;
    }

    if (startDate && !endDate) {
        const dayDiff = moment().diff(startDate, 'days');
        if (dayDiff <= 30) {
            return `Last ${dayDiff} days`;
        }
        return `From ${startDate.format('ll')}`;
    }

    return 'Unknown time range';
}
