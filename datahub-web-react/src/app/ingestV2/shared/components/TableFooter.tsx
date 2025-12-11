/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { TableCell, TableRow } from '@components/components/Table/components';

import { useUserContext } from '@app/context/useUserContext';
import HiddenItemsMessage from '@app/ingestV2/shared/components/HiddenItemsMessage';

const StyledTableCell = styled(TableCell)`
    padding: 0;
`;

interface Props {
    hiddenItemsMessage?: string;
    colSpan?: number;
}

export default function TableFooter({ hiddenItemsMessage, colSpan }: Props) {
    const me = useUserContext();

    // Do not show footer if user has permissions to manage Ingestion
    if (me.platformPrivileges?.manageIngestion) return null;

    return (
        <TableRow>
            <StyledTableCell alignment="center" colSpan={colSpan}>
                <HiddenItemsMessage message={hiddenItemsMessage} />
            </StyledTableCell>
        </TableRow>
    );
}
