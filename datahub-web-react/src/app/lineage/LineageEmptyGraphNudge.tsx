import { Alert } from '@components';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory, useLocation } from 'react-router-dom';
import { Panel } from 'reactflow';

import {
    AdjacencyList,
    LineageEmptyGraphNudgeNodes,
    LineageEmptyGraphNudgeReason,
} from '@app/lineage/utils/lineageEmptyGraphNudgeUtils';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import useLineageEmptyGraphNudge from '@app/lineage/utils/useLineageEmptyGraphNudge';
import updateQueryParams from '@app/shared/updateQueryParams';

export type LineageEmptyGraphNudgeProps = {
    rootUrn: string;
    adjacencyList: AdjacencyList;
    nodes: LineageEmptyGraphNudgeNodes;
    showGhostEntities: boolean;
    setShowGhostEntities: (value: boolean) => void;
};

export default function LineageEmptyGraphNudge({
    rootUrn,
    adjacencyList,
    nodes,
    showGhostEntities,
    setShowGhostEntities,
}: LineageEmptyGraphNudgeProps) {
    const { t } = useTranslation('lineage');
    const history = useHistory();
    const location = useLocation();
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const [dismissed, setDismissed] = useState(false);

    useEffect(() => {
        setDismissed(false);
    }, [rootUrn, startTimeMillis, endTimeMillis, showGhostEntities]);

    const { show, reasons, loading } = useLineageEmptyGraphNudge({
        rootUrn,
        adjacencyList,
        nodes,
        showGhostEntities,
    });

    const description = useMemo(() => getDescription(reasons, t), [reasons, t]);

    const onShowAllTime = useCallback(() => {
        updateQueryParams(
            {
                start_time_millis: undefined,
                end_time_millis: undefined,
                show_all_time_lineage: 'true',
            },
            location,
            history,
        );
    }, [location, history]);

    const action = useMemo(() => {
        if (reasons.includes('timeFilter')) {
            return {
                label: t('emptyGraphNudge.showAllTimeAction'),
                onClick: onShowAllTime,
                dataTestId: 'lineage-empty-graph-show-all-time',
            };
        }
        if (reasons.includes('hiddenEdges')) {
            return {
                label: t('emptyGraphNudge.showHiddenEdgesAction'),
                onClick: () => setShowGhostEntities(true),
                dataTestId: 'lineage-empty-graph-show-hidden-edges',
            };
        }
        return undefined;
    }, [reasons, t, onShowAllTime, setShowGhostEntities]);

    if (loading || !show || dismissed) {
        return null;
    }

    return (
        <Panel position="top-center">
            <Alert
                variant="brand"
                title={t('emptyGraphNudge.title')}
                description={description}
                action={action}
                onClose={() => setDismissed(true)}
                data-testid="lineage-empty-graph-nudge"
            />
        </Panel>
    );
}

function getDescription(reasons: LineageEmptyGraphNudgeReason[], t: (key: string) => string): string {
    const suggestions: string[] = [];

    if (reasons.includes('hiddenEdges')) {
        suggestions.push(t('emptyGraphNudge.suggestions.hiddenEdges'));
    }

    if (!suggestions.length) {
        return t('emptyGraphNudge.description');
    }

    return `${t('emptyGraphNudge.description')} ${suggestions.join(' ')}`;
}
