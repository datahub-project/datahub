/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { EntityAndType } from '@app/entity/shared/types';
import CompactContext from '@app/shared/CompactContext';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '@app/useEntityRegistry';

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
