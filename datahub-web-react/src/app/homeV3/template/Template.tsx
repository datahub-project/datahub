import { spacing } from '@components';
import React from 'react';
import styled from 'styled-components';

import AssetCollectionModal from '@app/homeV3/modules/assetCollection/AssetCollectionModal';
import useHandleAddModule from '@app/homeV3/modules/hooks/useHandleAddModule';
import useModulesAvailableToAdd from '@app/homeV3/modules/hooks/useModulesAvailableToAdd';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import TemplateRow from '@app/homeV3/templateRow/TemplateRow';

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
    const hasRows = !!template?.properties?.rows?.length;

    const modulesAvailableToAdd = useModulesAvailableToAdd();
    const { showAddAssetCollectionModal, setShowAddAssetCollectionModal, onAddModule } = useHandleAddModule();

    return (
        <>
            <Wrapper className={className}>
                {template?.properties?.rows.map((row, i) => {
                    const key = `templateRow-${i}`;
                    return (
                        <TemplateRow
                            key={key}
                            row={row}
                            rowIndex={i}
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
            {showAddAssetCollectionModal && (
                <AssetCollectionModal setShowAddAssetCollectionModal={setShowAddAssetCollectionModal} />
            )}
        </>
    );
}
