import { SimpleSelect } from '@components';
import moment from 'moment-timezone';
import React from 'react';
import styled from 'styled-components';

const SelectContainer = styled.div`
    max-width: 300px;
`;

type Props = {
    value: string;
    onChange: (newTimezone: any) => void;
};

export const TimezoneSelect = ({ value, onChange }: Props) => {
    const timezones = moment.tz.names();
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
                initialValues={[value]}
                showClear={false}
                width="full"
            />
        </SelectContainer>
    );
};
