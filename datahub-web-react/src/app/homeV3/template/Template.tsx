import { spacing } from '@components';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import useModulesAvailableToAdd from '@app/homeV3/modules/hooks/useModulesAvailableToAdd';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import { AddModuleHandlerInput } from '@app/homeV3/template/types';
import TemplateRow from '@app/homeV3/templateRow/TemplateRow';
import { wrapRows } from '@app/homeV3/templateRow/utils';

import { DataHubPageTemplate } from '@types';

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
    template: DataHubPageTemplate | null | undefined;
    className?: string;
}

export default function Template({ template, className }: Props) {
    const rows = useMemo(() => template?.properties?.rows ?? [], [template?.properties?.rows]);
    const hasRows = useMemo(() => !!rows.length, [rows.length]);
    const wrappedRows = useMemo(() => wrapRows(rows), [rows]);

    const onAddModule = useCallback((input: AddModuleHandlerInput) => {
        // TODO: implement the real handler
        console.log('onAddModule handled with input', input);
    }, []);

    const modulesAvailableToAdd = useModulesAvailableToAdd();

    return (
        <Wrapper className={className}>
            {wrappedRows.map((row, i) => {
                const key = `templateRow-${i}`;
                return (
                    <TemplateRow
                        key={key}
                        row={row}
                        rowIndex={i}
                        originRowIndex={row.originRowIndex}
                        modulesAvailableToAdd={modulesAvailableToAdd}
                        onAddModule={onAddModule}
                    />
                );
            })}
            <StyledAddModulesButton
                orientation="horizontal"
                $hasRows={hasRows}
                modulesAvailableToAdd={modulesAvailableToAdd}
                onAddModule={onAddModule}
            />
        </Wrapper>
    );
}
