import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { EntityAndType } from '../entity/shared/types';
import CompactContext from '../shared/CompactContext';
import EntitySidebarContext from '../shared/EntitySidebarContext';
import { useEntityRegistry } from '../useEntityRegistry';

const SidebarContainer = styled.div`
    height: calc(100vh - 60px - 40px - 20px - 10px);
    display: flex;
    flex-direction: column;
    position: sticky;
    top: 0;

    // hide the scrollbar
    ::-webkit-scrollbar {
        display: none; /* for Chrome, Safari and Opera */
    }
    -ms-overflow-style: none; /* IE and Edge */
    scrollbar-width: none; /* Firefox */
`;

interface Props {
    highlightedIndex: number | null;
    selectedEntity?: EntityAndType | null;
}

export const SearchEntitySidebarContainer = ({ highlightedIndex, selectedEntity }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [isClosed, setIsClosed] = useState(false);

    if (highlightedIndex === null) {
        return null;
    }

    return (
        <EntitySidebarContext.Provider
            value={{ isClosed, setSidebarClosed: setIsClosed, width: window.innerWidth * 0.3 }}
        >
            <SidebarContainer key={selectedEntity?.urn || ''}>
                {selectedEntity && (
                    <CompactContext.Provider value>
                        {entityRegistry.renderProfile(selectedEntity.type, selectedEntity.urn)}
                    </CompactContext.Provider>
                )}
            </SidebarContainer>
        </EntitySidebarContext.Provider>
    );
};
