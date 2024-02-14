import React, { useContext } from 'react';
import { matchPath, useLocation } from 'react-router';
import styled from 'styled-components';
import { PageRoutes } from '../../../conf/Global';
import EntitySidebarContext from '../../shared/EntitySidebarContext';
import TagStyleEntity from '../../shared/TagStyleEntity';
import { StyledEntitySidebarContainer, StyledSidebar } from '../shared/containers/profile/sidebar/EntityProfileSidebar';
import SidebarCollapseControls from '../shared/containers/profile/sidebar/SidebarCollapseControls';

const CompactEntityWrapper = styled.div`
    padding: 16px;
`;

interface Props {
    urn: string;
}

/**
 * Responsible for displaying metadata about a tag
 */
export default function CompactTagProfile({ urn }: Props) {
    const location = useLocation();
    const isInSearch = matchPath(location.pathname, PageRoutes.SEARCH_RESULTS) !== null;
    const { isClosed, width } = useContext(EntitySidebarContext);

    return (
        <StyledEntitySidebarContainer
            isCollapsed={isClosed}
            $width={width}
            id="entity-profile-sidebar"
            isCard={isInSearch}
        >
            <StyledSidebar isCard={isInSearch} isFocused={isInSearch}>
                <SidebarCollapseControls hideCollapseViewDetails />
                {!isClosed && (
                    <CompactEntityWrapper>
                        <TagStyleEntity urn={urn} />
                    </CompactEntityWrapper>
                )}
            </StyledSidebar>
        </StyledEntitySidebarContainer>
    );
}
