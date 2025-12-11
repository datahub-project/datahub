/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SidebarHeader } from '@app/entity/shared/containers/profile/sidebar/SidebarHeader';
import { ErModelRelationship } from '@src/types.generated';

const CardinalitySidebar = styled.div`
    color: #000000;
    font-weight: 400;
    display: flex;
    flex-flow: column nowrap;
`;

const ERModelRelationshipCardinality = styled.span`
    display: flex;
    align-items: center;
    gap: 10px;
`;

const ERModelRelationshipSidebarCardinality = () => {
    const { entityData } = useEntityData();
    return (
        <CardinalitySidebar>
            <SidebarHeader title="Cardinality" />
            <ERModelRelationshipCardinality>
                <>{(entityData as ErModelRelationship)?.properties?.cardinality}</>
            </ERModelRelationshipCardinality>
        </CardinalitySidebar>
    );
};

export default ERModelRelationshipSidebarCardinality;
