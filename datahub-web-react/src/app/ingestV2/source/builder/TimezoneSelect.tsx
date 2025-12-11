/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Select } from 'antd';
import moment from 'moment-timezone';
import React from 'react';
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
