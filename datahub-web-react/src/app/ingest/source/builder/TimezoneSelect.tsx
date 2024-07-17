import { Select } from 'antd';
import React from 'react';
import moment from 'moment-timezone';
import styled from 'styled-components';

const StyledSelect = styled(Select)`
    max-width: 300px;
`;

type Props = {
    value: string;
    onChange: (newTimezone: any) => void;
};

export const TimezoneSelect = ({ value, onChange }: Props) => {
    const timezones = moment.tz.names();
    return (
        <>
            <StyledSelect showSearch value={value} onChange={onChange}>
                {timezones.map((timezone) => (
                    <Select.Option key={timezone} value={timezone}>
                        {timezone}
                    </Select.Option>
                ))}
            </StyledSelect>
        </>
    );
};
