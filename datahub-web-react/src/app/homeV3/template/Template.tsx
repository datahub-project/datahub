import { spacing } from '@components';
import React, { memo, useMemo } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import ModuleModalMapper from '@app/homeV3/moduleModals/ModuleModalMapper';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import DragAndDropProvider from '@app/homeV3/template/components/DragAndDropProvider';
import TemplateGrid from '@app/homeV3/template/components/TemplateGrid';
import { BottomAddButtonMode, getBottomButtonMode } from '@app/homeV3/template/components/utils';
import { wrapRows } from '@app/homeV3/templateRow/utils';

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
    const hasRows = useMemo(() => !!rows.length, [rows.length]);
    const wrappedRows = useMemo(() => wrapRows(rows), [rows]);

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
