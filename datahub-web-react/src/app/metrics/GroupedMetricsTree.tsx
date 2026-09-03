import { EmptyState } from '@components';
import { Sigma } from '@phosphor-icons/react/dist/csr/Sigma';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import MetricsGroupSection from '@app/metrics/MetricsGroupSection';
import { useMetricsEntityContext } from '@app/metrics/context/MetricsEntityContext';
import useMetricsGroupRoots from '@app/metrics/useMetricsGroupRoots';
import { GroupedMetricsMode, resolveActiveMetricsGroup } from '@app/metrics/utils/metricsSidebarGrouping';
import { MetricsSidebarSortValue } from '@app/metrics/utils/metricsSidebarSort';
import { useEntityRegistry } from '@app/useEntityRegistry';

const EmptyStateWrapper = styled.div`
    display: flex;
    justify-content: center;
    padding: 24px 12px;
`;

type Props = {
    mode: GroupedMetricsMode;
    sort: MetricsSidebarSortValue;
};

export default function GroupedMetricsTree({ mode, sort }: Props) {
    const { t } = useTranslation('misc');
    const entityRegistry = useEntityRegistry();
    const { entityData, selectedUrn } = useMetricsEntityContext();
    const [expandedGroupKeys, setExpandedGroupKeys] = useState<Set<string>>(new Set());

    const activeGroup = useMemo(
        () =>
            resolveActiveMetricsGroup(entityData, selectedUrn, mode, t('metrics.groupBy.unassigned'), (entity) =>
                entityRegistry.getDisplayName(entity.type, entity),
            ),
        [entityData, entityRegistry, mode, selectedUrn, t],
    );

    const { groups, loading } = useMetricsGroupRoots(mode, t('metrics.groupBy.unassigned'), activeGroup);

    /**
     * Groups start collapsed: each expanded group runs its own paginated scroll
     * query, so expanding every group up front would fire one request per
     * platform/domain at once. Only the active entity's group is opened below.
     */
    useEffect(() => {
        if (!activeGroup) return;
        setExpandedGroupKeys((current) => {
            if (current.has(activeGroup.key)) return current;
            return new Set(current).add(activeGroup.key);
        });
    }, [activeGroup]);

    if (!loading && groups.length === 0) {
        return (
            <EmptyStateWrapper>
                <EmptyState
                    icon={Sigma}
                    title={t('metrics.emptyTreeTitle')}
                    description={t('metrics.emptyTreeDescription')}
                    size="sm"
                />
            </EmptyStateWrapper>
        );
    }

    return (
        <div data-testid={`metrics-sidebar-${mode}-groups`}>
            {groups.map((group) => (
                <MetricsGroupSection
                    key={group.key}
                    group={group}
                    sort={sort}
                    isExpanded={expandedGroupKeys.has(group.key)}
                    selectedUrn={selectedUrn}
                    onToggle={() =>
                        setExpandedGroupKeys((current) => {
                            const next = new Set(current);
                            if (next.has(group.key)) next.delete(group.key);
                            else next.add(group.key);
                            return next;
                        })
                    }
                />
            ))}
        </div>
    );
}
