import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { EntityAndType } from '../entity/shared/types';
import CompactContext from '../shared/CompactContext';
import EntitySidebarContext from '../sharedV2/EntitySidebarContext';
import useSidebarWidth from '../sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '../useEntityRegistry';

const SidebarContainer = styled.div<{ height: string }>`
    max-height: ${(props) => props.height};
    display: flex;
    flex-direction: column;
    position: sticky;
    top: 0;
`;

interface Props {
    height: string;
    highlightedIndex: number | null;
    selectedEntity?: EntityAndType | null;
}

export const SearchEntitySidebarContainer = ({ height, highlightedIndex, selectedEntity }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [isClosed, setIsClosed] = useState(false);
    const width = useSidebarWidth();

    if (highlightedIndex === null) {
        return null;
    }

    return (
        <EntitySidebarContext.Provider value={{ width, isClosed, setSidebarClosed: setIsClosed }}>
            <SidebarContainer key={selectedEntity?.urn || ''} height={height}>
                {selectedEntity && (
                    <CompactContext.Provider value>
                        {entityRegistry.renderProfile(selectedEntity.type, selectedEntity.urn)}
                    </CompactContext.Provider>
                )}
            </SidebarContainer>
        </EntitySidebarContext.Provider>
    );
};
