import React from 'react';

import { useUserContext } from '@app/context/useUserContext';
import TableFooterRow from '@app/ingestV2/shared/components/TableFooterRow';
import HiddenSourcesMessage from '@app/ingestV2/source/components/HiddenSourcesMessage';

interface Props {
    colSpan?: number;
}

export default function TableFooter({ colSpan }: Props) {
    const me = useUserContext();

    // Do not show footer if user has permissions to manage Ingestion
    if (me.platformPrivileges?.manageIngestion) return null;

    return (
        <TableFooterRow colSpan={colSpan}>
            <HiddenSourcesMessage />
        </TableFooterRow>
    );
}
