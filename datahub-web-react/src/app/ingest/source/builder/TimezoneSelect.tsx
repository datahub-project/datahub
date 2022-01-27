import { Select } from 'antd';
import React from 'react';
import moment from 'moment-timezone';

type Props = {
    value: string;
    onChange: (newTimezone: string) => void;
};

export const TimezoneSelect = ({ value, onChange }: Props) => {
    const timezones = moment.tz.names();
    return (
        <>
            <Select showSearch value={value} onChange={onChange}>
                {timezones.map((timezone) => (
                    <Select.Option value={timezone}>{timezone}</Select.Option>
                ))}
            </Select>
        </>
    );
};
