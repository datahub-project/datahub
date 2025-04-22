import { Button } from 'antd';
import React, { useContext } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SEARCH_COLORS } from '@app/entityV2/shared/constants';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import SidebarBackArrow from '@images/sidebarBackArrow.svg?react';

const Container = styled.div`
    display: flex;
    flex-direction: horizontal;
    justify-content: space-between;
    padding-left: 20px;
    padding-right: 20px;
    padding-top: 12px;
    padding-bottom: 4px;
    align-items: center;
`;

const StyledSidebarBackArrow = styled(SidebarBackArrow)`
    cursor: pointer;
`;

type Props = {
    showViewDetails?: boolean;
};

export default function EntityProfileSidebarSearchHeader({ showViewDetails = true }: Props) {
    const entitySidebarContext = useContext(EntitySidebarContext);
    const entityRegistry = useEntityRegistry();
    const { urn, entityType } = useEntityData();

    return (
        <Container>
            <StyledSidebarBackArrow
                onClick={() => {
                    entitySidebarContext.setSidebarClosed(true);
                }}
            />
            {showViewDetails && (
                <Button
                    size="small"
                    type="primary"
                    color={SEARCH_COLORS.TITLE_PURPLE}
                    href={entityRegistry.getEntityUrl(entityType, urn)}
                >
                    View more
                </Button>
            )}
        </Container>
    );
}
