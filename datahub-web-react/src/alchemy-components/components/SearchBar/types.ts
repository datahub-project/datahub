/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

export interface SearchBarProps {
    placeholder?: string;
    value?: string;
    width?: string;
    height?: string;
    onChange?: (value: string, event: React.ChangeEvent<HTMLInputElement>) => void;
    allowClear?: boolean;
    clearIcon?: React.ReactNode;
    disabled?: boolean;
    suffix?: React.ReactNode;
    forceUncontrolled?: boolean;
    onCompositionStart?: React.CompositionEventHandler<HTMLInputElement>;
    onCompositionEnd?: React.CompositionEventHandler<HTMLInputElement>;
}
