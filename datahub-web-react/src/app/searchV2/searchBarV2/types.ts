/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { EntityType } from '@src/types.generated';

export interface Option {
    value: string;
    label: React.ReactNode;
    type?: string | EntityType;
    style?: React.CSSProperties;
    disabled?: boolean;
}

export interface SectionOption extends Omit<Option, 'value'> {
    value?: string;
    options?: Option[];
}
