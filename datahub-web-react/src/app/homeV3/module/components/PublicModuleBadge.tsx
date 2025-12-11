/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Pill } from '@components';
import React from 'react';

interface Props {
    isPublic?: boolean;
}

export default function PublicModuleBadge({ isPublic }: Props) {
    if (!isPublic) return null;

    return <Pill label="Public" color="gray" size="xs" />;
}
