/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { TreeNodeProps } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

const Wrapper = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
    padding: 8px;
`;

export default function GlossaryTreeNodeRenderer({ node }: TreeNodeProps) {
    const entityRegistry = useEntityRegistryV2();
    const name = entityRegistry.getDisplayName(node.entity.type, node.entity);

    return (
        <Wrapper>
            <EntityIcon entity={node.entity} />
            <Text weight="semiBold">{name}</Text>
        </Wrapper>
    );
}
