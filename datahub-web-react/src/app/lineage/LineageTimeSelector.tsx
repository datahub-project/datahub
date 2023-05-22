import React, { useEffect, useState } from 'react';
import moment from 'moment';
import styled from 'styled-components/macro';
import { DatePicker, Tooltip } from 'antd';
import { CalendarOutlined, CaretDownOutlined } from '@ant-design/icons';
import ClickOutside from '../shared/ClickOutside';
import { getTimeRangeDescription } from '../shared/time/timeUtils';
import { ANTD_GRAY } from '../entity/shared/constants';

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
    initialDates: [moment.Moment | null, moment.Moment | null];
};

export default function LineageTimeSelector({ onChange, initialDates }: Props) {
    const [isOpen, setIsOpen] = useState(false);
    const [headerText, setHeaderText] = useState('No time range selected');
    const [startDate, setStartDate] = useState<moment.Moment | null>(initialDates[0]);
    const [endDate, setEndDate] = useState<moment.Moment | null>(initialDates[1]);

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
                            return current && current > moment().endOf('day');
                        }}
                        format="ll"
                        ranges={{
                            'Last 7 days': [moment().subtract(7, 'days'), moment()],
                            'Last 14 days': [moment().subtract(14, 'days'), moment()],
                            'Last 28 days': [moment().subtract(28, 'days'), moment()],
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
