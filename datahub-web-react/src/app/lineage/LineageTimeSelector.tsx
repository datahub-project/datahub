import { CalendarOutlined, CaretDownOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import dayjs from 'dayjs';
import type { Dayjs } from 'dayjs';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import ClickOutside from '@app/shared/ClickOutside';
import { getTimeRangeDescription } from '@app/shared/time/timeUtils';
import DatePicker from '@utils/DayjsDatePicker';

const RangePickerWrapper = styled.div`
    transition: color 0s;
    position: relative;
    &:hover {
        background-color: ${ANTD_GRAY[2]};
        cursor: pointer;
    }
    .ant-picker-range {
        visibility: hidden;
        position: absolute;
        top: 0;
    }
`;

const Header = styled.div`
    padding: 6px;
    display: flex;
    align-items: center;
`;

const { RangePicker } = DatePicker;

type Props = {
    onChange: (dates, _dateStrings) => void;
    initialDates: [Dayjs | null, Dayjs | null];
};

export default function LineageTimeSelector({ onChange, initialDates }: Props) {
    const [isOpen, setIsOpen] = useState(false);
    const [headerText, setHeaderText] = useState('No time range selected');
    const [startDate, setStartDate] = useState<Dayjs | null>(initialDates[0]);
    const [endDate, setEndDate] = useState<Dayjs | null>(initialDates[1]);

    useEffect(() => {
        const timeRangeDescription = getTimeRangeDescription(startDate, endDate);
        setHeaderText(timeRangeDescription);
    }, [startDate, endDate]);

    return (
        <Tooltip title="Filter lineage edges by observed date" placement="topLeft">
            <RangePickerWrapper>
                <ClickOutside onClickOutside={() => setIsOpen(false)}>
                    <Header onClick={() => setIsOpen(!isOpen)}>
                        <CalendarOutlined style={{ marginRight: '4px' }} />
                        <b>{headerText}</b> &nbsp;
                        <CaretDownOutlined style={{ fontSize: '10px' }} />
                    </Header>
                    <RangePicker
                        allowClear
                        allowEmpty={[true, true]}
                        bordered={false}
                        value={initialDates}
                        disabledDate={(current: any) => {
                            return current && current > dayjs().endOf('day');
                        }}
                        format="ll"
                        ranges={{
                            'Last 7 days': [dayjs().subtract(7, 'days'), dayjs()],
                            'Last 14 days': [dayjs().subtract(14, 'days'), dayjs()],
                            'Last 28 days': [dayjs().subtract(28, 'days'), dayjs()],
                            'All Time': [null, null],
                        }}
                        onChange={(dates, dateStrings) => {
                            if (dates) {
                                setStartDate(dates[0]);
                                setEndDate(dates[1]);
                            }
                            onChange(dates, dateStrings);
                            setIsOpen(false);
                        }}
                        open={isOpen}
                        getPopupContainer={(trigger) => trigger.parentElement || trigger}
                    />
                </ClickOutside>
            </RangePickerWrapper>
        </Tooltip>
    );
}
