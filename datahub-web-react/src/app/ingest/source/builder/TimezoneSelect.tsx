import { Select } from 'antd';
import React from 'react';
import moment from 'moment-timezone';

type Props = {
    value: string;
    onChange: (newTimezone: string) => void;
    disabled?: boolean;
};

export const TimezoneSelect = ({ value, onChange, ...props }: Props) => {
    const timezones = moment.tz.names();
    return (
        <Select showSearch value={value} onChange={onChange} {...props}>
            {timezones.map((timezone) => (
                <Select.Option value={timezone}>{timezone}</Select.Option>
            ))}
        </Select>
    );
};
