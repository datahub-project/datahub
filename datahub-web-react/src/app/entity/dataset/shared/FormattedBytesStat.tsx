/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from 'antd';
import React from 'react';

import { formatBytes, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

export const FormattedBytesStat = ({ bytes }: { bytes: number }) => {
    const formattedBytes = formatBytes(bytes);
    return (
        <Tooltip title={`This dataset consumes ${formatNumberWithoutAbbreviation(bytes)} bytes of storage.`}>
            <b>{formattedBytes.number}</b> {formattedBytes.unit}
        </Tooltip>
    );
};
