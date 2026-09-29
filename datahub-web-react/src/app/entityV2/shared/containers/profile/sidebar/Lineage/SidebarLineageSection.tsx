import { Button, Icon, Tooltip } from '@components';
import { ArrowDown } from '@phosphor-icons/react/dist/csr/ArrowDown';
import { ArrowUp } from '@phosphor-icons/react/dist/csr/ArrowUp';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import React, { useContext } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import SidebarLineageLoadingSection from '@app/entityV2/shared/containers/profile/sidebar/Lineage/SidebarLineageLoadingSection';
import { useSearchSummaryLineage } from '@app/entityV2/shared/containers/profile/sidebar/Lineage/SidebarLineageSection.hooks';
import {
    LineageDirectionSummary,
    getDirectDownstreamSummary,
    getDirectUpstreamSummary,
    getRelatedEntitySummary,
} from '@app/entityV2/shared/containers/profile/sidebar/Lineage/utils';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { TabContextType } from '@app/entityV2/shared/types';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { useGetDefaultLineageStartTimeMillis } from '@app/lineage/utils/useGetLineageTimeParams';
import { formatNumber } from '@app/shared/formatNumber';
import { useEntityRegistry } from '@app/useEntityRegistry';
import UpstreamHealth from '@src/app/entityV2/shared/embed/UpstreamHealth/UpstreamHealth';
import CompactContext from '@src/app/shared/CompactContext';

import { useGetSearchAcrossLineageCountsQuery } from '@graphql/lineage.generated';

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

type Props = {
    contexType?: TabContextType;
};

const SidebarLineageSection = ({ contexType }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const { urn, entityData, entityType } = useEntityData();
    const { isInFormContext } = useEntityFormContext();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const isCompact = useContext(CompactContext);
    const startTimeMillis = useGetDefaultLineageStartTimeMillis();

    const separateSiblings = useIsSeparateSiblingsMode();
    const onCombinedSiblingPage = !separateSiblings && (entityData?.siblingsSearch?.total || 0) > 0;
    const isSearchSummary = contexType === TabContextType.SEARCH_SIDEBAR;
    const {
        directUpstreamCount: searchUpstreamCount,
        directDownstreamCount: searchDownstreamCount,
        upstreamTypeSummary,
        downstreamTypeSummary,
        loading: searchLoading,
    } = useSearchSummaryLineage({
        enabled: isSearchSummary,
        urn,
        entityData,
        separateSiblings,
        startTimeMillis,
        skip: onCombinedSiblingPage,
    });
    const { data: profileData, loading: profileLoading } = useGetSearchAcrossLineageCountsQuery({
        variables: { urn, startTimeMillis },
        fetchPolicy: 'cache-first',
        skip: isSearchSummary || onCombinedSiblingPage,
    });

    const directUpstreamSummary = isSearchSummary
        ? upstreamTypeSummary
        : profileData?.upstreams && getDirectUpstreamSummary(profileData.upstreams as any);
    const directDownstreamSummary = isSearchSummary
        ? downstreamTypeSummary
        : profileData?.downstreams && getDirectDownstreamSummary(profileData.downstreams as any);

    const directUpstreamCount = isSearchSummary ? searchUpstreamCount : directUpstreamSummary?.total || 0;
    const directDownstreamCount = isSearchSummary ? searchDownstreamCount : directDownstreamSummary?.total || 0;
    const loading = isSearchSummary ? searchLoading : profileLoading;

    const hasLineage = directUpstreamCount > 0 || directDownstreamCount > 0;

    if (!hasLineage) {
        return null;
    }

    const renderSummary = (i18nKey: string, count: number, summary?: LineageDirectionSummary | null) => (
        <Trans
            t={t}
            i18nKey={i18nKey}
            components={{
                summary: summary ? (
                    (getRelatedEntitySummary(summary, entityRegistry) as React.ReactElement)
                ) : (
                    <>{t('entityCount.asset', { count, formattedCount: formatNumber(count) })}</>
                ),
            }}
        />
    );
    const upstreamSummary = renderSummary('sidebar.lineage.dependsOn', directUpstreamCount, directUpstreamSummary);
    const downstreamSummary = renderSummary('sidebar.lineage.usedBy', directDownstreamCount, directDownstreamSummary);

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
                            <SummaryText>{upstreamSummary}</SummaryText>
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
                            <SummaryText>{downstreamSummary}</SummaryText>
                        </Section>
                    )}
                </>
            }
            extra={
                <>
                    {!isInFormContext && (
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
                    )}
                </>
            }
        />
    );
};

export default SidebarLineageSection;
