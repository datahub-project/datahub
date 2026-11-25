import { spacing } from '@components';
import React, { memo, useMemo } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import ModuleModalMapper from '@app/homeV3/moduleModals/ModuleModalMapper';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import DragAndDropProvider from '@app/homeV3/template/components/DragAndDropProvider';
import TemplateGrid from '@app/homeV3/template/components/TemplateGrid';
import { BottomAddButtonMode, getBottomButtonMode } from '@app/homeV3/template/components/utils';
import { useIsModuleHidden } from '@app/homeV3/templateRow/hooks/useIsModuleHidden';
import { getVisibleRows, wrapRows } from '@app/homeV3/templateRow/utils';

import { DataHubPageTemplateRow } from '@types';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.xxsm};
`;

// Additional margin to have width of content excluding side buttons
const StyledAddModulesButton = styled(AddModuleButton)<{ $mode: BottomAddButtonMode }>`
    ${({ $mode }) => {
        if ($mode === 'homeWithRows') {
            return 'margin: 0 42px;';
        }
        if ($mode === 'assetSummary') {
            return 'margin: 0 6px;';
        }
        return '';
    }}
`;

interface Props {
    className?: string;
}

function Template({ className }: Props) {
    const { urn } = useEntityData();
    const { templateType, template, isTemplateEditable, moduleContext } = usePageTemplateContext();
    const rows = useMemo(
        () => (template?.properties?.rows ?? []) as DataHubPageTemplateRow[],
        [template?.properties?.rows],
    );
    const wrappedRows = useMemo(() => wrapRows(rows), [rows]);
    const isModuleHidden = useIsModuleHidden();
    // A template whose rows are all hidden (e.g. only the MCP module while MCP is off) draws like an
    // empty one, so the bottom add button is the add control and is laid out for an empty page.
    const hasRows = useMemo(
        () => getVisibleRows(wrappedRows, isModuleHidden).length > 0,
        [wrappedRows, isModuleHidden],
    );

    return (
        // set data-testid once module context resolves to reduce cypress flakiness
        <Wrapper
            className={className}
            key={urn}
            data-testid={moduleContext.globalTemplate ? 'template-wrapper' : undefined}
        >
            <DragAndDropProvider>
                <TemplateGrid wrappedRows={wrappedRows} />
            </DragAndDropProvider>

            {isTemplateEditable && (
                <StyledAddModulesButton orientation="horizontal" $mode={getBottomButtonMode(templateType, hasRows)} />
            )}
            <ModuleModalMapper />
        </Wrapper>
    );
}

export default memo(Template);
