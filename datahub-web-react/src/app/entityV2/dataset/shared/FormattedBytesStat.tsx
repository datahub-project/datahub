import React from 'react';
import { Tooltip } from '@components';
import { formatBytes, formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';

export const FormattedBytesStat = ({ bytes }: { bytes: number }) => {
    const formattedBytes = formatBytes(bytes);
    return (
        <Tooltip title={`This dataset consumes ${formatNumberWithoutAbbreviation(bytes)} bytes of storage.`}>
            <b>{formattedBytes.number}</b> {formattedBytes.unit}
        </Tooltip>
    );
};
