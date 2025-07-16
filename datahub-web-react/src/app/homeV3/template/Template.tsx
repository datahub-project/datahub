import { spacing } from '@components';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import useModulesAvailableToAdd from '@app/homeV3/modules/hooks/useModulesAvailableToAdd';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import TemplateRow from '@app/homeV3/templateRow/TemplateRow';
import { wrapRows } from '@app/homeV3/templateRow/utils';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.md};
`;

// Additional margin to have width of content excluding side buttons
const StyledAddModulesButton = styled(AddModuleButton)<{ $hasRows?: boolean }>`
    ${(props) => props.$hasRows && 'margin: 0 48px;'}
`;

interface Props {
    className?: string;
}

export default function Template({ className }: Props) {
    const { template } = usePageTemplateContext();
    const rows = useMemo(() => template?.properties?.rows ?? [], [template?.properties?.rows]);
    const hasRows = useMemo(() => !!rows.length, [rows.length]);
    const wrappedRows = useMemo(() => wrapRows(rows), [rows]);

    const modulesAvailableToAdd = useModulesAvailableToAdd();

    return (
        <Wrapper className={className}>
            {wrappedRows.map((row, i) => {
                const key = `templateRow-${i}`;
                return <TemplateRow key={key} row={row} rowIndex={i} modulesAvailableToAdd={modulesAvailableToAdd} />;
            })}
            <StyledAddModulesButton
                orientation="horizontal"
                $hasRows={hasRows}
                modulesAvailableToAdd={modulesAvailableToAdd}
            />
        </Wrapper>
    );
}
