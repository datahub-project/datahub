import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Domain, EntityType, RecommendationContent } from '../../../../types.generated';
import { IconStyleType } from '../../../entity/Entity';
import { LogoCountCard } from '../../../shared/LogoCountCard';
import { useEntityRegistry } from '../../../useEntityRegistry';

const DomainListContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    flex-wrap: wrap;
`;

type Props = {
    content: Array<RecommendationContent>;
    onClick?: (index: number) => void;
};

export const DomainSearchList = ({ content, onClick }: Props) => {
    const entityRegistry = useEntityRegistry();

    const domainsWithCounts: Array<{ domain: Domain; count?: number }> = content
        .map((cnt) => ({ domain: cnt.entity, count: cnt.params?.contentParams?.count }))
        .filter((domainWithCount) => domainWithCount.domain !== null && domainWithCount !== undefined) as Array<{
        domain: Domain;
        count?: number;
    }>;

    return (
        <DomainListContainer>
            {domainsWithCounts.map((domain, index) => (
                <Link
                    to={entityRegistry.getEntityUrl(EntityType.Domain, domain.domain.urn)}
                    key={domain.domain.urn}
                    onClick={() => onClick?.(index)}
                >
                    <LogoCountCard
                        name={entityRegistry.getDisplayName(EntityType.Domain, domain.domain)}
                        logoComponent={entityRegistry.getIcon(EntityType.Domain, 16, IconStyleType.ACCENT)}
                        count={domain.count}
                    />
                </Link>
            ))}
        </DomainListContainer>
    );
};
