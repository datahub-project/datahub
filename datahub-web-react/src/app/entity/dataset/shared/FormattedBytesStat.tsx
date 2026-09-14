import { Tooltip } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { formatBytes, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

export const FormattedBytesStat = ({ bytes }: { bytes: number }) => {
    const { t } = useTranslation('entity.shared.containers');
    const formattedBytes = formatBytes(bytes);
    return (
        <Tooltip
            title={t('sidebar.datasetHeader.storageSizeTooltip', {
                count: bytes,
                formattedCount: formatNumberWithoutAbbreviation(bytes),
            })}
        >
            <b>{formattedBytes.number}</b> {formattedBytes.unit}
        </Tooltip>
    );
};
