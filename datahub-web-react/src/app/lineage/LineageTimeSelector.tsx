import { CalendarOutlined, CaretDownOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import ClickOutside from '@app/shared/ClickOutside';
import { getTimeRangeDescription } from '@app/shared/time/timeUtils';
import DatePicker from '@utils/DayjsDatePicker';
import dayjs from '@utils/dayjs';
import type { Dayjs } from '@utils/dayjs';

const RangePickerWrapper = styled.div`
    transition: color 0s;
    position: relative;
    &:hover {
        background-color: ${(props) => props.theme.colors.bgSurface};
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
    const { t } = useTranslation('lineage');
    const [isOpen, setIsOpen] = useState(false);
    const [headerText, setHeaderText] = useState(() => t('timeSelector.noTimeRangeSelected'));
    const [startDate, setStartDate] = useState<Dayjs | null>(initialDates[0]);
    const [endDate, setEndDate] = useState<Dayjs | null>(initialDates[1]);

    useEffect(() => {
        const timeRangeDescription = getTimeRangeDescription(startDate, endDate);
        setHeaderText(timeRangeDescription);
    }, [startDate, endDate]);

    return (
        <Tooltip title={t('timeSelector.filterTooltip')} placement="topLeft">
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
                            [t('timeSelector.last7Days')]: [dayjs().subtract(7, 'days'), dayjs()],
                            [t('timeSelector.last14Days')]: [dayjs().subtract(14, 'days'), dayjs()],
                            [t('timeSelector.last28Days')]: [dayjs().subtract(28, 'days'), dayjs()],
                            [t('timeSelector.allTime')]: [null, null],
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
