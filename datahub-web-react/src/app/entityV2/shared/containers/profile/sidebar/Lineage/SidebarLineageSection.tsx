import { Button, Icon, Tooltip } from '@components';
import { ArrowDown } from '@phosphor-icons/react/dist/csr/ArrowDown';
import { ArrowUp } from '@phosphor-icons/react/dist/csr/ArrowUp';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import React, { useContext } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { matchPath, useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import SidebarLineageLoadingSection from '@app/entityV2/shared/containers/profile/sidebar/Lineage/SidebarLineageLoadingSection';
import {
    getDirectDownstreamSummary,
    getDirectUpstreamSummary,
    getRelatedEntitySummary,
} from '@app/entityV2/shared/containers/profile/sidebar/Lineage/utils';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { TabContextType } from '@app/entityV2/shared/types';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { useGetDefaultLineageStartTimeMillis } from '@app/lineage/utils/useGetLineageTimeParams';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';
import UpstreamHealth from '@src/app/entityV2/shared/embed/UpstreamHealth/UpstreamHealth';
import CompactContext from '@src/app/shared/CompactContext';

import { useGetLineageCountsQuery, useGetSearchAcrossLineageCountsQuery } from '@graphql/lineage.generated';

const Section = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    margin-bottom: 6px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const DirectionText = styled.div`
    font-size: 10px;
    font-weight: 700;
    line-height: 20px;
    letter-spacing: 0.48px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const SummaryText = styled.div`
    text-wrap: wrap;
    font-size: 12px;
    font-weight: 600;
    line-height: 20px;
`;

const DirectionIcon = styled.span`
    margin-right: 4px;
    display: flex;
    align-items: center;
`;

const DirectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    font-weight: bold;
    font-size: 12px;
    letter-spacing: 1px;
    height: 20px;
    color: ${(props) => props.theme.colors.textTertiary};
    min-width: 100px;
    margin-right: 6px;
`;

type LineageCount = {
    filtered: number;
    total: number;
};

type LineageCountEntity = {
    upstream?: LineageCount | null;
    downstream?: LineageCount | null;
};

type Props = {
    contexType?: TabContextType;
};

const getVisibleCount = (count?: LineageCount | null): number => (count?.total || 0) - (count?.filtered || 0);

const SidebarLineageSection = ({ contexType }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const { urn, entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const location = useLocation();
    const isCompact = useContext(CompactContext);
    const startTimeMillis = useGetDefaultLineageStartTimeMillis();

    const separateSiblings = useIsSeparateSiblingsMode();
    const onCombinedSiblingPage = !separateSiblings && (entityData?.siblingsSearch?.total || 0) > 0;
    const isSearchSummary =
        contexType === TabContextType.SEARCH_SIDEBAR ||
        matchPath(location.pathname, PageRoutes.SEARCH_RESULTS) !== null;
    const { data: searchData, loading: searchLoading } = useGetLineageCountsQuery({
        variables: { urn, separateSiblings, startTimeMillis },
        fetchPolicy: 'cache-first',
        skip: !isSearchSummary || onCombinedSiblingPage,
    });
    const { data: profileData, loading: profileLoading } = useGetSearchAcrossLineageCountsQuery({
        variables: { urn, startTimeMillis },
        fetchPolicy: 'cache-first',
        skip: isSearchSummary || onCombinedSiblingPage,
    });

    const directUpstreamSummary = profileData?.upstreams && getDirectUpstreamSummary(profileData.upstreams as any);
    const directDownstreamSummary =
        profileData?.downstreams && getDirectDownstreamSummary(profileData.downstreams as any);
    const searchLineage = searchData?.entity as LineageCountEntity | null | undefined;

    const directUpstreamCount = isSearchSummary
        ? getVisibleCount(searchLineage?.upstream)
        : directUpstreamSummary?.total || 0;
    const directDownstreamCount = isSearchSummary
        ? getVisibleCount(searchLineage?.downstream)
        : directDownstreamSummary?.total || 0;
    const loading = isSearchSummary ? searchLoading : profileLoading;

    const hasLineage = directUpstreamCount > 0 || directDownstreamCount > 0;

    if (!hasLineage) {
        return null;
    }

    return (
        <SidebarSection
            title={t('sidebar.lineage.sectionTitle')}
            key="Lineage"
            content={
                <>
                    {loading && <SidebarLineageLoadingSection />}
                    {!loading && !isSearchSummary && <UpstreamHealth />}
                    {!loading && directUpstreamCount > 0 && (
                        <Section key="upstream">
                            <Tooltip title={t('sidebar.lineage.upstreamTooltip')} placement="left" showArrow={false}>
                                <DirectionHeader>
                                    <DirectionIcon>
                                        <Icon icon={ArrowUp} size="md" />
                                    </DirectionIcon>
                                    <DirectionText>{t('sidebar.lineage.upstreamLabel')}</DirectionText>
                                </DirectionHeader>
                            </Tooltip>
                            <SummaryText>
                                {isSearchSummary ? (
                                    directUpstreamCount
                                ) : (
                                    <Trans
                                        t={t}
                                        i18nKey="sidebar.lineage.dependsOn"
                                        components={{
                                            summary: getRelatedEntitySummary(
                                                directUpstreamSummary as any,
                                                entityRegistry,
                                            ) as React.ReactElement,
                                        }}
                                    />
                                )}
                            </SummaryText>
                        </Section>
                    )}
                    {!loading && directDownstreamCount > 0 && (
                        <Section key="downstream">
                            <Tooltip title={t('sidebar.lineage.downstreamTooltip')} placement="left" showArrow={false}>
                                <DirectionHeader>
                                    <DirectionIcon>
                                        <Icon icon={ArrowDown} size="md" />
                                    </DirectionIcon>
                                    <DirectionText>{t('sidebar.lineage.downstreamLabel')}</DirectionText>
                                </DirectionHeader>
                            </Tooltip>
                            <SummaryText>
                                {isSearchSummary ? (
                                    directDownstreamCount
                                ) : (
                                    <Trans
                                        t={t}
                                        i18nKey="sidebar.lineage.usedBy"
                                        components={{
                                            summary: getRelatedEntitySummary(
                                                directDownstreamSummary as any,
                                                entityRegistry,
                                            ) as React.ReactElement,
                                        }}
                                    />
                                )}
                            </SummaryText>
                        </Section>
                    )}
                </>
            }
            extra={
                <Tooltip title={t('sidebar.lineage.exploreGraphTooltip')} placement="left" showArrow={false}>
                    <Button
                        variant="text"
                        color="primary"
                        size="md"
                        icon={{ icon: TreeStructure }}
                        onClick={(e) => {
                            e.stopPropagation();
                            const lineagePath = `${entityRegistry.getEntityUrl(entityType, urn)}/Lineage`;
                            if (isCompact) {
                                window.open(lineagePath, '_blank');
                            } else {
                                history.push(lineagePath);
                            }
                        }}
                    />
                </Tooltip>
            }
        />
    );
};

export default SidebarLineageSection;
