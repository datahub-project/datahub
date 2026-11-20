import { Icon } from '@components';
import { ArrowUUpLeft } from '@phosphor-icons/react/dist/csr/ArrowUUpLeft';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { ItemType } from '@components/components/Menu/types';

import {
    EXECUTION_REQUEST_STATUS_RUNNING,
    EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';
import { ExecutionRequestRecord } from '@app/ingestV2/executions/types';
import BaseActionsColumn from '@app/ingestV2/shared/components/columns/BaseActionsColumn';

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
    const items: ItemType[] = [
        {
            type: 'item',
            key: 'copy-urn',
            title: t('executions.copyUrn'),
            disabled: !record.urn || !navigator.clipboard,
            onClick: () => {
                navigator.clipboard.writeText(record.urn);
            },
        },
        {
            type: 'item',
            key: 'details',
            title: tl('details'),
            onClick: () => {
                handleViewDetails(record.urn);
            },
        },
    ];

    if (record.status === EXECUTION_REQUEST_STATUS_RUNNING) {
        items.push({
            type: 'item',
            key: 'cancel',
            title: tc('cancel'),
            onClick: () => handleCancel(record.urn),
        });
    }

    return (
        <BaseActionsColumn
            dropdownItems={items}
            extraActions={
                (record.status === EXECUTION_REQUEST_STATUS_SUCCESS ||
                    record.status === EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS) &&
                record.showRollback ? (
                    <Icon
                        icon={ArrowUUpLeft}
                        size="lg"
                        onClick={() => handleRollback(record.id)}
                        tooltipText={t('executions.rollback')}
                    />
                ) : null
            }
        />
    );
}
