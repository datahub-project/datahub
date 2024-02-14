import React, { useState } from 'react';
import { Panel } from 'reactflow';
import styled from 'styled-components';
import moment from 'moment';
import { DatePicker } from 'antd';

const { RangePicker } = DatePicker;

const StyledControlsPanel = styled(Panel)<{ isRootPanelExpanded: boolean }>`
    margin-top: 36px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    padding: 16px;
    width: 255px;
    border-radius: 8px;
    border: 1px solid #d5d5d5;
    background: #fff;
    box-shadow: 0px 4px 4px 0px rgba(224, 224, 224, 0.25);
    overflow: hidden;
    margin-left: ${({ isRootPanelExpanded }) => (isRootPanelExpanded ? '178px' : '66px')};
    transition: margin-left 0.3s ease-in-out;
`;

const TimeRangeTitleText = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: #0958d9;
`;

const TimeRangeSubText = styled.div`
    color: #595959;
    font-size: 10px;
    font-weight: 500;
    margin-bottom: 8px;
`;

type Props = {
    isRootPanelExpanded: boolean;
};

const LineageTimeRangeControls = ({ isRootPanelExpanded }: Props) => {
    const [isOpen, setIsOpen] = useState(false);
    const [, setStartDate] = useState<moment.Moment | null>(null);
    const [, setEndDate] = useState<moment.Moment | null>(null);

    return (
        <StyledControlsPanel position="top-left" isRootPanelExpanded={isRootPanelExpanded}>
            <TimeRangeTitleText>Time Range</TimeRangeTitleText>
            <TimeRangeSubText>Show edges that were observed after the start date and before the end.</TimeRangeSubText>
            <RangePicker
                // style={{ padding: '0px' }}
                allowClear
                onClick={() => setIsOpen(true)}
                allowEmpty={[true, true]}
                bordered={false}
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
                onChange={(dates, _dateStrings) => {
                    if (dates) {
                        setStartDate(dates[0]);
                        setEndDate(dates[1]);
                    }
                    if (dates?.[0] && dates?.[1]) {
                        setIsOpen(false);
                    }
                    // onChange(dates, dateStrings);
                    // setIsOpen(false);
                }}
                // placeholder={['After', 'Before']}
                open={isOpen}
                // getPopupContainer={(trigger) => trigger.parentElement || trigger}
            />
        </StyledControlsPanel>
    );
};

export default LineageTimeRangeControls;
