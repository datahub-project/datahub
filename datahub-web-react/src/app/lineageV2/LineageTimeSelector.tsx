import { CalendarOutlined, CaretDownOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Button, Space } from 'antd';
import i18next from 'i18next';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import DatePicker from '@utils/DayjsDatePicker';
import dayjs from '@utils/dayjs';
import type { Dayjs } from '@utils/dayjs';

const { RangePicker } = DatePicker;

export type Datetime = Dayjs | null;

const TimeRangeTrigger = styled.button`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    border: none;
    background: none;
    padding: 0;
    font-size: 12px;
    font-weight: 700;
    line-height: 16px;
    color: ${(props) => props.theme.colors.text};
    cursor: pointer;
`;

const TriggerCaret = styled(CaretDownOutlined)`
    font-size: 10px;
    color: ${(props) => props.theme.colors.icon};
`;

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

type Props = {
    onChange: (start: Datetime, end: Datetime) => void;
    startTimeMillis?: number;
    endTimeMillis?: number;
};

export default function LineageTimeSelector({ onChange, startTimeMillis, endTimeMillis }: Props) {
    const { t } = useTranslation('lineage');
    const { t: tcAction } = useTranslation('common.actions');
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
            {showText ? (
                <Tooltip title={t('timeSelector.filterTooltip')} placement="topLeft" showArrow={false}>
                    <TimeRangeTrigger type="button" onClick={() => handleOpenChange(true)}>
                        <CalendarOutlined style={{ marginRight: '4px' }} />
                        {getTimeRangeDescription(startDate, endDate)}
                        <TriggerCaret />
                    </TimeRangeTrigger>
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
                                    {tcAction('confirm')}
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
        return i18next.t('lineage:timeSelector.allTime');
    }

    if (!startDate && endDate) {
        return i18next.t('lineage:timeSelector.until', { date: endDate.format('ll') });
    }

    if (startDate && !endDate) {
        const dayDiff = dayjs().diff(startDate, 'days');
        if (dayDiff <= 30) {
            return i18next.t('lineage:timeSelector.lastNDays', { count: dayDiff });
        }
        return i18next.t('lineage:timeSelector.from', { date: startDate.format('ll') });
    }

    return i18next.t('lineage:timeSelector.unknownRange');
}
