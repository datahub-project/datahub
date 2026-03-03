import { Database } from '@phosphor-icons/react';
import { VerticalDivider } from '@remirror/react';
import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entityV2/Entity';
import { pluralize } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { SearchResultFields_Domain_Fragment } from '@graphql/search.generated';
import { EntityType } from '@types';

const Wrapper = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    display: flex;
    align-items: center;

    svg {
        margin-right: 4px;
    }
`;

const StyledDivider = styled(VerticalDivider)`
    &&& {
        margin: 0 8px;
    }
`;

interface Props {
    domain: SearchResultFields_Domain_Fragment;
}

export default function DomainEntitiesSnippet({ domain }: Props) {
    const entityRegistry = useEntityRegistry();
    const entityCount = domain.entities?.total || 0;
    const subDomainCount = domain.children?.total || 0;
    const dataProductCount = domain.dataProducts?.total || 0;

    return (
        <Wrapper>
            <Database size={12} color="currentColor" /> {entityCount} {entityCount === 1 ? 'entity' : 'entities'}
            <StyledDivider />
            {entityRegistry.getIcon(EntityType.Domain, 12, IconStyleType.ACCENT)} {subDomainCount}{' '}
            {pluralize(subDomainCount, 'sub-domain')}
            <StyledDivider />
            {entityRegistry.getIcon(EntityType.DataProduct, 12, IconStyleType.ACCENT)} {dataProductCount}{' '}
            {pluralize(dataProductCount, 'data product')}
        </Wrapper>
    );
}
