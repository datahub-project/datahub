import { ExportOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { SummaryTabWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';
import { toLocalDateString, toRelativeTimeString } from '@app/sharedV2/time/timeUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetRepositoryQuery } from '@graphql/repository.generated';
import { EntityType } from '@types';

const FactsStrip = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
`;

const Fact = styled.span`
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 2px 10px;
    border-radius: 8px;
    font-size: 12px;
    font-weight: 600;
    border: 1px solid ${(props) => props.theme.colors.border};
    color: ${(props) => props.theme.colors.textSecondary};
`;

const ExternalLink = styled.a`
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.hyperlinks};
    &:hover {
        text-decoration: underline;
    }
`;

/**
 * The default landing tab for a Repository. Leads with a repo facts strip
 * (languages, license, branch, visibility, archived, external link, fork-of),
 * then About/README.
 */
export const RepositorySummaryTab = () => {
    const { t } = useTranslation('entity.types');
    const entityRegistry = useEntityRegistry();
    const baseEntity = useBaseEntity<GetRepositoryQuery>();
    const repo = baseEntity?.repository;

    const repoProps = repo?.properties;
    const source = repo?.source;
    const externalUrl = source?.externalUrl || repoProps?.homepageUrl;
    const forkOf = repo?.forkOf;

    return (
        <SummaryTabWrapper>
            <FactsStrip data-testid="repository-summary-facts">
                {repoProps?.languages?.map((lang) => <Fact key={lang}>{lang}</Fact>)}
                {repoProps?.license && <Fact>{repoProps.license}</Fact>}
                {repoProps?.defaultBranch && <Fact>{repoProps.defaultBranch}</Fact>}
                {repoProps?.archived && <Fact>{t('repository.summary.archived')}</Fact>}
                {repoProps?.created?.time && (
                    <Tooltip showArrow={false} title={toLocalDateString(repoProps.created.time)}>
                        <Fact>
                            {`${t('repository.summary.created')} ${toRelativeTimeString(repoProps.created.time)}`}
                        </Fact>
                    </Tooltip>
                )}
                {repoProps?.lastModified?.time && (
                    <Tooltip showArrow={false} title={toLocalDateString(repoProps.lastModified.time)}>
                        <Fact>
                            {`${t('repository.summary.lastModified')} ${toRelativeTimeString(repoProps.lastModified.time)}`}
                        </Fact>
                    </Tooltip>
                )}
                {externalUrl && (
                    <ExternalLink href={externalUrl} target="_blank" rel="noopener noreferrer">
                        <ExportOutlined />
                        {t('repository.summary.viewSource')}
                    </ExternalLink>
                )}
                {forkOf && (
                    <Fact>
                        {`${t('repository.summary.forkOf')} `}
                        <Link to={entityRegistry.getEntityUrl(EntityType.Repository, forkOf.urn)}>
                            {forkOf.properties?.name || forkOf.urn}
                        </Link>
                    </Fact>
                )}
            </FactsStrip>

            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
};
