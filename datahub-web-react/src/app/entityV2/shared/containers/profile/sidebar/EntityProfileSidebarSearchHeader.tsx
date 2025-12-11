/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button } from 'antd';
import React, { useContext } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useCustomTheme } from '@src/customThemeContext';

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
    const { theme } = useCustomTheme();
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
                    color={theme?.styles['primary-color']}
                    href={entityRegistry.getEntityUrl(entityType, urn)}
                >
                    View more
                </Button>
            )}
        </Container>
    );
}
