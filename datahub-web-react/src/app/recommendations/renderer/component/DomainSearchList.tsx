/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ArrowRightOutlined } from '@ant-design/icons';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import DomainIcon from '@app/domain/DomainIcon';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { LogoCountCard } from '@app/shared/LogoCountCard';
import { HomePageButton } from '@app/shared/components';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import { Domain, EntityType, RecommendationContent } from '@types';

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
