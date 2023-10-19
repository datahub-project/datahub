import { ArrowRightOutlined } from '@ant-design/icons';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Domain, EntityType, RecommendationContent } from '../../../../types.generated';
import { LogoCountCard } from '../../../shared/LogoCountCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import DomainIcon from '../../../domain/DomainIcon';
import { PageRoutes } from '../../../../conf/Global';
import { HomePageButton } from '../../../shared/components';
import { HoverEntityTooltip } from './HoverEntityTooltip';

const DomainListContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    flex-wrap: wrap;
`;

const AllDomainsWrapper = styled.div`
    color: ${(props) => props.theme.styles['primary-color']};
    font-size: 14px;
`;

const AllDomainsText = styled.div`
    margin-bottom: 8px;
`;

const NUM_DOMAIN_CARDS = 9;

type Props = {
    content: Array<RecommendationContent>;
    onClick?: (index: number) => void;
};

export const DomainSearchList = ({ content, onClick }: Props) => {
    const entityRegistry = useEntityRegistry();

    const domainsWithCounts: Array<{ domain: Domain; count?: number }> = content
        .map((cnt) => ({ domain: cnt.entity, count: cnt.params?.contentParams?.count }))
        .filter((domainWithCount) => domainWithCount?.domain !== null)
        .slice(0, NUM_DOMAIN_CARDS) as Array<{
        domain: Domain;
        count?: number;
    }>;

    return (
        <DomainListContainer>
            {domainsWithCounts.map((domain, index) => (
                <HoverEntityTooltip key={domain.domain.urn} entity={domain.domain} placement="bottom">
                    <Link
                        to={entityRegistry.getEntityUrl(EntityType.Domain, domain.domain.urn)}
                        onClick={() => onClick?.(index)}
                    >
                        <LogoCountCard
                            name={entityRegistry.getDisplayName(EntityType.Domain, domain.domain)}
                            logoComponent={
                                <DomainIcon
                                    style={{
                                        fontSize: 16,
                                        color: '#BFBFBF',
                                    }}
                                />
                            }
                            count={domain.count}
                        />
                    </Link>
                </HoverEntityTooltip>
            ))}
            <Link to={PageRoutes.DOMAINS}>
                <HomePageButton type="link">
                    <AllDomainsWrapper>
                        <AllDomainsText>View All Domains</AllDomainsText>
                        <ArrowRightOutlined />
                    </AllDomainsWrapper>
                </HomePageButton>
            </Link>
        </DomainListContainer>
    );
};
