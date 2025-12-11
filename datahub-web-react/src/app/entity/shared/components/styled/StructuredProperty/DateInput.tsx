/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DatePicker } from 'antd';
import moment, { Moment } from 'moment';
import React from 'react';

interface Props {
    selectedValues: any[];
    updateSelectedValues: (values: string[] | number[]) => void;
}

export default function DateInput({ selectedValues, updateSelectedValues }: Props) {
    function updateInput(_: Moment | null, value: string) {
        updateSelectedValues([value]);
    }

    const currentValue = selectedValues[0] ? moment(selectedValues[0]) : undefined;

    return <DatePicker onChange={updateInput} value={currentValue} />;
}
