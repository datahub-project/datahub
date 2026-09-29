import { SimpleSelect } from '@components';
import React from 'react';
import styled from 'styled-components';

import { getSupportedTimezones } from '@app/shared/time/timeUtils';

const SelectContainer = styled.div`
    max-width: 300px;
`;

type Props = {
    value: string;
    onChange: (newTimezone: string) => void;
    disabled?: boolean;
    label?: string;
};

export const TimezoneSelect = ({ value, onChange, disabled, label }: Props) => {
    const timezones = getSupportedTimezones();
    const options = timezones.map((timezone) => {
        return {
            value: timezone,
            label: timezone,
        };
    });

    return (
        <SelectContainer>
            <SimpleSelect
                options={options}
                showSearch
                onUpdate={(values) => onChange(values[0])}
                values={[value]}
                isDisabled={disabled}
                showClear={false}
                width="full"
                label={label}
            />
        </SelectContainer>
    );
};
