/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Input } from 'antd';
import React from 'react';

interface Props {
    name: string;
    value: string;
    onChangeValue: (newValue: string) => void;
}

export default function TextValueInput({ name, value, onChangeValue }: Props) {
    return (
        <Input
            placeholder={`Enter ${name.toLocaleLowerCase()}`}
            data-testid="edit-text-input"
            onChange={(e) => onChangeValue(e.target.value)}
            value={value}
        />
    );
}
