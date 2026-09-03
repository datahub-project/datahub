import { Cube } from '@phosphor-icons/react/dist/csr/Cube';
import { FolderSimple } from '@phosphor-icons/react/dist/csr/FolderSimple';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import { DocumentSourceLogo } from '@app/document/DocumentSourceLogo';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import { MetricsTreeItem } from '@app/metrics/MetricsTreeItem';
import { GroupedMetricsEntity } from '@app/metrics/metricsTypes';
import useMetricsGroupEntities from '@app/metrics/useMetricsGroupEntities';
import { METRICS_GROUP_BY, MetricsGroup } from '@app/metrics/utils/metricsSidebarGrouping';
import { MetricsSidebarSortValue } from '@app/metrics/utils/metricsSidebarSort';
import { TreeSectionHeader } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeSectionHeader';
import { PageRoutes } from '@conf/Global';

const ParentContext = styled.span`
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 12px;
`;

const GroupIcon = styled.span`
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
`;

type Props = {
    group: MetricsGroup;
    sort: MetricsSidebarSortValue;
    isExpanded: boolean;
    selectedUrn: string | null;
    onToggle: () => void;
};

function getEntityTitle(entity: GroupedMetricsEntity): string {
    return entity.info?.name ?? entity.urn;
}

export default function MetricsGroupSection({ group, sort, isExpanded, selectedUrn, onToggle }: Props) {
    const { t } = useTranslation('misc');
    const history = useHistory();
    const theme = useTheme();
    const { entities, scrollRef } = useMetricsGroupEntities({
        mode: group.mode,
        groupKey: group.key,
        sort,
        skip: !isExpanded,
    });

    let icon = <FolderSimple color={theme.colors.icon} size={16} />;
    if (group.mode === METRICS_GROUP_BY.PLATFORM && group.entity) {
        icon = <DocumentSourceLogo platform={group.entity} size={16} fallback={null} />;
    } else if (group.mode === METRICS_GROUP_BY.DOMAIN && group.entity) {
        icon = <DomainColoredIcon domain={group.entity} size={20} fontSize={12} />;
    }

    const navigateToEntity = (entity: GroupedMetricsEntity) => {
        const route = entity.__typename === 'Metric' ? PageRoutes.METRIC_ENTITY : PageRoutes.SEMANTIC_MODEL_ENTITY;
        history.push(`${route}/${encodeURIComponent(entity.urn)}`);
    };

    return (
        <>
            <TreeSectionHeader
                level={0}
                label={group.label}
                icon={<GroupIcon>{icon}</GroupIcon>}
                isExpanded={isExpanded}
                onToggle={onToggle}
                testId={`metrics-sidebar-group-${group.key}`}
            />
            {isExpanded &&
                entities.map((entity) => {
                    const isMetric = entity.__typename === 'Metric';
                    const parentName = isMetric ? entity.semanticModel?.info?.name : null;
                    return (
                        <MetricsTreeItem
                            key={entity.urn}
                            level={1}
                            icon={isMetric ? Sigma : Cube}
                            title={getEntityTitle(entity)}
                            isSelected={selectedUrn === entity.urn}
                            afterLabel={
                                parentName ? (
                                    <ParentContext>
                                        {t('metrics.groupBy.parentContext', { name: parentName })}
                                    </ParentContext>
                                ) : undefined
                            }
                            onClick={() => navigateToEntity(entity)}
                            testId={`metrics-sidebar-group-entity-${entity.urn}`}
                        />
                    );
                })}
            {isExpanded && <div ref={scrollRef} style={{ height: 1 }} />}
        </>
    );
}
