import { Text } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { Popover } from '@src/alchemy-components';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetMetricQuery } from '@graphql/metric.generated';
import { EntityType, Metric, SemanticModel } from '@types';

const MAX_VISIBLE_SEGMENTS = 5;

const BreadcrumbRow = styled.div`
    display: flex;
    align-items: center;
    flex-wrap: nowrap;
    gap: 4px;
    overflow: hidden;
    padding: 0 2px 8px;
`;

const Separator = styled(Text).attrs({ size: 'sm' })`
    color: ${(props) => props.theme.colors.textSecondary};
    flex-shrink: 0;
`;

const BreadcrumbLink = styled(Link)`
    font-size: 13px;
    color: ${(props) => props.theme.colors.textSecondary};
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 200px;
    display: inline-block;

    &:hover {
        color: ${(props) => props.theme.colors.text};
        text-decoration: underline;
    }
`;

const BreadcrumbLabel = styled(Text).attrs({ size: 'sm' })`
    color: ${(props) => props.theme.colors.textSecondary};
    white-space: nowrap;
    flex-shrink: 0;
`;

const EllipsisButton = styled.span`
    font-size: 13px;
    color: ${(props) => props.theme.colors.textSecondary};
    cursor: pointer;
    padding: 0 2px;
    border-radius: 4px;

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
    }
`;

const PopoverList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 6px;
    padding: 4px 0;
`;

const PopoverLink = styled(Link)`
    font-size: 13px;
    color: ${(props) => props.theme.colors.text};

    &:hover {
        text-decoration: underline;
    }
`;

type BreadcrumbSegment =
    | { kind: 'label'; text: string }
    | { kind: 'link'; text: string; url: string }
    | { kind: 'ellipsis'; hidden: Array<{ text: string; url: string }> };

const ELLIPSIS = '\u2026';
const ELLIPSIS_KEY = 'ellipsis-segment';

function collectParentChain(metric: Metric | null | undefined): Array<{ urn: string; name: string }> {
    const chain: Array<{ urn: string; name: string }> = [];
    let current = metric?.parentMetric as Metric | null | undefined;
    while (current) {
        chain.unshift({ urn: current.urn, name: current.info?.name ?? current.urn });
        current = current.parentMetric as Metric | null | undefined;
    }
    return chain;
}

function buildBreadcrumbSegments(
    entityRegistry: ReturnType<typeof useEntityRegistry>,
    metric: Metric | null | undefined,
    semanticModel: SemanticModel | null | undefined,
): BreadcrumbSegment[] {
    const metricName = metric?.info?.name ?? metric?.id ?? metric?.urn ?? '';
    const smUrl = semanticModel
        ? entityRegistry.getEntityUrl(EntityType.SemanticModel, semanticModel.urn)
        : null;
    const smName = semanticModel?.info?.name ?? semanticModel?.urn ?? '';

    const parents = collectParentChain(metric);

    // Build the "collapsible middle" items (SM + parent chain).
    const smItems: Array<{ text: string; url: string }> = smUrl && smName ? [{ text: smName, url: smUrl }] : [];
    const parentItems: Array<{ text: string; url: string }> = parents.map((p) => ({
        text: p.name,
        url: entityRegistry.getEntityUrl(EntityType.Metric, p.urn),
    }));
    const middleItems = [...smItems, ...parentItems];

    // Total visible segments: label + middle + current = 1 + middle.length + 1
    const totalCount = 1 + middleItems.length + 1;
    const segments: BreadcrumbSegment[] = [{ kind: 'label', text: 'Metric' }];

    if (totalCount <= MAX_VISIBLE_SEGMENTS) {
        middleItems.forEach((item) => {
            segments.push({ kind: 'link', text: item.text, url: item.url });
        });
    } else {
        // Show first item (SM), ellipsis for skipped middle items, last item before current.
        const firstVisible = middleItems[0];
        const lastVisible = middleItems[middleItems.length - 1];
        const hidden = middleItems.slice(1, middleItems.length - 1);

        if (firstVisible) {
            segments.push({ kind: 'link', text: firstVisible.text, url: firstVisible.url });
        }
        if (hidden.length > 0) {
            segments.push({ kind: 'ellipsis', hidden });
        }
        if (lastVisible && lastVisible !== firstVisible) {
            segments.push({ kind: 'link', text: lastVisible.text, url: lastVisible.url });
        }
    }

    segments.push({ kind: 'label', text: metricName });

    return segments;
}

export default function MetricHeader() {
    const theme = useTheme();
    const entityRegistry = useEntityRegistry();
    const baseEntity = useBaseEntity<GetMetricQuery>();
    const metric = baseEntity?.metric as Metric | null | undefined;
    const semanticModel = metric?.semanticModel as SemanticModel | null | undefined;

    const segments = buildBreadcrumbSegments(entityRegistry, metric, semanticModel);

    if (segments.length <= 1) {
        return null;
    }

    return (
        <BreadcrumbRow data-testid="metric-header-breadcrumb">
            {segments.map((seg, idx) => {
                let segKey: string;
                if (seg.kind === 'label') {
                    segKey = `label-${seg.text}`;
                } else if (seg.kind === 'link') {
                    segKey = `link-${seg.url}`;
                } else {
                    segKey = ELLIPSIS_KEY;
                }
                return (
                    <React.Fragment key={segKey}>
                        {idx > 0 && <Separator style={{ color: theme.colors.textSecondary }}>/</Separator>}
                        {seg.kind === 'label' && <BreadcrumbLabel>{seg.text}</BreadcrumbLabel>}
                        {seg.kind === 'link' && <BreadcrumbLink to={seg.url}>{seg.text}</BreadcrumbLink>}
                        {seg.kind === 'ellipsis' && (
                            <Popover
                                content={
                                    <PopoverList>
                                        {seg.hidden.map((item) => (
                                            <PopoverLink key={item.url} to={item.url}>
                                                {item.text}
                                            </PopoverLink>
                                        ))}
                                    </PopoverList>
                                }
                                placement="bottom"
                            >
                                <EllipsisButton>{ELLIPSIS}</EllipsisButton>
                            </Popover>
                        )}
                    </React.Fragment>
                );
            })}
        </BreadcrumbRow>
    );
}
