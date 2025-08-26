import { DatePicker, Dropdown } from 'antd';
import React, { useEffect, useRef } from 'react';

type Props = {
    open: boolean;
    onOpenChange: (open: boolean) => void;
    children: React.ReactNode;
    onChange: (window: { startTime: number; endTime: number }) => void;
};

export const CustomTimeRangeSelectDropdown = ({ open, onOpenChange, children, onChange }: Props) => {
    const timeRangePickerRef = useRef<any>(null);

    /**
     * Invoked when user selects new custom time range (e.g. 1/1/2020 12:00 AM - 1/31/2020 11:59 PM)
     */
    const onRangeChange = (dates) => {
        if (dates && dates[0] && dates[1]) {
            const [start, end] = dates;
            onChange({
                startTime: start.valueOf(),
                endTime: end.valueOf(),
            });
        }
    };

    useEffect(() => {
        if (open) {
            setTimeout(() => timeRangePickerRef.current?.focus(), 100);
        }
    }, [open]);

    return (
        <Dropdown
            open={open}
            onOpenChange={onOpenChange}
            trigger={['click']}
            dropdownRender={() => (
                <DatePicker.RangePicker
                    ref={timeRangePickerRef}
                    showTime
                    format="MMM D, h:mm A"
                    onOk={onRangeChange}
                    onChange={onRangeChange}
                />
            )}
        >
            {children}
        </Dropdown>
    );
};
