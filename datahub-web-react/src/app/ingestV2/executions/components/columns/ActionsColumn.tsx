import React from 'react';

import { EXECUTION_REQUEST_STATUS_RUNNING } from '@app/ingestV2/executions/constants';
import { ExecutionRequestRecord } from '@app/ingestV2/executions/types';
import BaseActionsColumn, { MenuItem } from '@app/ingestV2/shared/components/columns/BaseActionsColumn';

interface ActionsColumnProps {
    record: ExecutionRequestRecord;
    setFocusExecutionUrn: (urn: string) => void;
}

export function ActionsColumn({ record, setFocusExecutionUrn }: ActionsColumnProps) {
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
                    Copy URN
                </MenuItem>
            ),
        },
        {
            key: '1',
            label: (
                <MenuItem
                    onClick={() => {
                        setFocusExecutionUrn(record.urn);
                    }}
                >
                    Details
                </MenuItem>
            ),
        },
        {
            key: '2',
            disabled: record.status !== EXECUTION_REQUEST_STATUS_RUNNING,
            label: <MenuItem onClick={() => {}}>Cancel</MenuItem>,
        },
    ];
    return <BaseActionsColumn dropdownItems={items} />;
}
