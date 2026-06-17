import { Database } from '@phosphor-icons/react/dist/csr/Database';
import { VerticalDivider } from '@remirror/react';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { IconStyleType } from '@app/entityV2/Entity';
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
    const { t } = useTranslation('entity.types');
    const entityRegistry = useEntityRegistry();
    const entityCount = domain.entities?.total || 0;
    const subDomainCount = domain.children?.total || 0;
    const dataProductCount = domain.dataProducts?.total || 0;

    return (
        <Wrapper>
            <Database size={12} color="currentColor" /> {t('domain.entitiesCount', { count: entityCount })}
            <StyledDivider />
            {entityRegistry.getIcon(EntityType.Domain, 12, IconStyleType.ACCENT)}{' '}
            {t('domain.subDomainsCount', { count: subDomainCount })}
            <StyledDivider />
            {entityRegistry.getIcon(EntityType.DataProduct, 12, IconStyleType.ACCENT)}{' '}
            {t('domain.dataProductsCount', { count: dataProductCount })}
        </Wrapper>
    );
}
