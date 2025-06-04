import React from 'react';
import styled from 'styled-components';

import { TableCell, TableRow } from '@components/components/Table/components';

const StyledTableCell = styled(TableCell)`
    padding: 0;
`;

interface Props {
    colSpan?: number;
}

export default function TableFooterRow({ colSpan, children }: React.PropsWithChildren<Props>) {
    return (
        <TableRow>
            <StyledTableCell alignment="center" colSpan={colSpan}>
                {children}
            </StyledTableCell>
        </TableRow>
    );
}
