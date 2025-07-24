import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { TreeNodeProps } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const Wrapper = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
    padding: 8px;
`;

export default function DomainSelectableTreeNodeRenderer({ node }: TreeNodeProps) {
    const entityRegistry = useEntityRegistryV2();

    const name = entityRegistry.getDisplayName(EntityType.Domain, node.entity);

    return (
        <Wrapper>
            <EntityIcon entity={node.entity} />
            <Text weight="semiBold">{name}</Text>
        </Wrapper>
    );
}
