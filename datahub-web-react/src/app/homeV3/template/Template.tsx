import { spacing } from '@components';
import React, { memo, useMemo } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import ModuleModalMapper from '@app/homeV3/moduleModals/ModuleModalMapper';
import useModulesAvailableToAdd from '@app/homeV3/modules/hooks/useModulesAvailableToAdd';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import DragAndDropProvider from '@app/homeV3/template/components/DragAndDropProvider';
import TemplateGrid from '@app/homeV3/template/components/TemplateGrid';
import { wrapRows } from '@app/homeV3/templateRow/utils';

import { DataHubPageTemplateRow } from '@types';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.xxsm};
`;

// Additional margin to have width of content excluding side buttons
const StyledAddModulesButton = styled(AddModuleButton)<{ $hasRows?: boolean }>`
    ${(props) => props.$hasRows && 'margin: 0 48px;'}
`;

interface Props {
    className?: string;
}

function Template({ className }: Props) {
    const { template } = usePageTemplateContext();
    const rows = useMemo(
        () => (template?.properties?.rows ?? []) as DataHubPageTemplateRow[],
        [template?.properties?.rows],
    );
    const hasRows = useMemo(() => !!rows.length, [rows.length]);
    const wrappedRows = useMemo(() => wrapRows(rows), [rows]);
    const modulesAvailableToAdd = useModulesAvailableToAdd();

    return (
        <Wrapper className={className}>
            <DragAndDropProvider>
                <TemplateGrid wrappedRows={wrappedRows} modulesAvailableToAdd={modulesAvailableToAdd} />
            </DragAndDropProvider>

            <StyledAddModulesButton
                orientation="horizontal"
                $hasRows={hasRows}
                modulesAvailableToAdd={modulesAvailableToAdd}
            />
            <ModuleModalMapper />
        </Wrapper>
    );
}

export default memo(Template);
