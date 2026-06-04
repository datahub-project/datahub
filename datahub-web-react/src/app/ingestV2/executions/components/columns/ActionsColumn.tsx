import { Icon } from '@components';
import { ArrowUUpLeft } from '@phosphor-icons/react/dist/csr/ArrowUUpLeft';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { EXECUTION_REQUEST_STATUS_RUNNING, EXECUTION_REQUEST_STATUS_SUCCESS } from '@app/ingestV2/executions/constants';
import { ExecutionRequestRecord } from '@app/ingestV2/executions/types';
import BaseActionsColumn, { MenuItem } from '@app/ingestV2/shared/components/columns/BaseActionsColumn';

interface ActionsColumnProps {
    record: ExecutionRequestRecord;
    handleViewDetails: (urn: string) => void;
    handleRollback: (urn: string) => void;
    handleCancel: (urn: string) => void;
}

export function ActionsColumn({ record, handleViewDetails, handleRollback, handleCancel }: ActionsColumnProps) {
    const { t } = useTranslation('ingestion');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');
    const items = [
        {
            key: '0',
            disabled: !record.urn || !navigator.clipboard,
            label: (
                <MenuItem
                    onClick={() => {
                        navigator.clipboard.writeText(record.urn);
                    }}
                >
                    {t('executions.copyUrn')}
                </MenuItem>
            ),
        },
        {
            key: '1',
            label: (
                <MenuItem
                    onClick={() => {
                        handleViewDetails(record.urn);
                    }}
                >
                    {tl('details')}
                </MenuItem>
            ),
        },
    ];

    if (record.status === EXECUTION_REQUEST_STATUS_RUNNING) {
        items.push({
            key: '2',
            label: <MenuItem onClick={() => handleCancel(record.urn)}>{tc('cancel')}</MenuItem>,
        });
    }

    return (
        <BaseActionsColumn
            dropdownItems={items}
            extraActions={
                record.status === EXECUTION_REQUEST_STATUS_SUCCESS && record.showRollback ? (
                    <Icon
                        icon={ArrowUUpLeft}
                        onClick={() => handleRollback(record.id)}
                        tooltipText={t('executions.rollback')}
                    />
                ) : null
            }
        />
    );
}
