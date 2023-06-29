import React from 'react';
import { Tooltip } from 'antd';
import { formatBytes, formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';

export const FormattedBytesStat = ({ bytes, disableTooltip = false }: { bytes: number; disableTooltip?: boolean }) => {
    const formattedBytes = formatBytes(bytes);
    return (
        <Tooltip
            title={`This dataset consumes ${formatNumberWithoutAbbreviation(bytes)} bytes of storage.`}
            open={disableTooltip ? false : undefined}
        >
            <b>{formattedBytes.number}</b> {formattedBytes.unit}
        </Tooltip>
    );
};
