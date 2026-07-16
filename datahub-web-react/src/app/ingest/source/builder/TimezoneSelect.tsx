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
};

export const TimezoneSelect = ({ value, onChange }: Props) => {
    const timezones = getSupportedTimezones();
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
