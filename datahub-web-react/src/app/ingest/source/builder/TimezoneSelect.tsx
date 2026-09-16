import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { getSupportedTimezones } from '@app/shared/time/timeUtils';

const StyledSelect = styled(Select)`
    max-width: 300px;
`;

type Props = {
    value: string;
    onChange: (newTimezone: any) => void;
    disabled?: boolean;
};

export const TimezoneSelect = ({ value, onChange, disabled, ..._props }: Props) => {
    const timezones = getSupportedTimezones();
    return (
        <>
            <StyledSelect showSearch value={value} onChange={onChange} disabled={disabled}>
                {timezones.map((timezone) => (
                    <Select.Option key={timezone} value={timezone}>
                        {timezone}
                    </Select.Option>
                ))}
            </StyledSelect>
        </>
    );
};
