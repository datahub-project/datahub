import { Icon } from '@components';
import React from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { Button, DownstreamWrapper, UpstreamWrapper } from '@app/lineageV3/LineageEntityNode/components';
import { useOnClickExpandLineage } from '@app/lineageV3/LineageEntityNode/useOnClickExpandLineage';
import { FetchStatus, onClickPreventSelect } from '@app/lineageV3/common';
import { useAppConfig } from '@app/useAppConfig';

import { EntityType, LineageDirection } from '@types';

const CountWrapper = styled.span<{ direction: LineageDirection }>`
    ${({ direction }) => direction === LineageDirection.Upstream && 'transform: scaleX(-1);'}
`;

interface Props {
    urn: string;
    type: EntityType;
    direction: LineageDirection;
    display: boolean;
    fetchStatus: Record<LineageDirection, FetchStatus>;
    count?: number;
    parentDataJob?: string; // Only one expansion per direction allowed among nodes with the same parent data job
    ignoreSchemaFieldStatus: boolean;
}

export function ExpandLineageButton({
    urn,
    type,
    direction,
    display,
    fetchStatus,
    count,
    parentDataJob,
    ignoreSchemaFieldStatus,
}: Props) {
    const { config } = useAppConfig();
    const expandOneLevel = useOnClickExpandLineage(urn, type, direction, false, parentDataJob);
    const expandAll = useOnClickExpandLineage(urn, type, direction, true, parentDataJob);
    const isFetchComplete = fetchStatus[direction] === FetchStatus.COMPLETE;
    const showExpandAll =
        config.featureFlags.showLineageExpandMore &&
        !count &&
        !isFetchComplete &&
        (type === EntityType.SchemaField ? !ignoreSchemaFieldStatus : true);

    const handleExpandOneLevel = (e: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
        expandOneLevel(e);
        analytics.event({
            type: EventType.ExpandLineageEvent,
            direction,
            entityUrn: urn,
            entityType: type,
            levelsExpanded: '1',
        });
    };

    const handleExpandAll = (e: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
        expandAll(e);
        analytics.event({
            type: EventType.ExpandLineageEvent,
            direction,
            entityUrn: urn,
            entityType: type,
            levelsExpanded: 'all',
        });
    };

    // Still have to render this component while request is loading, otherwise it gets cancelled
    // But don't render the buttons while the request is in progress
    // TODO: Reset fetch status if the request fails
    if (!display) return null;

    const Wrapper = direction === LineageDirection.Upstream ? UpstreamWrapper : DownstreamWrapper;

    return (
        <Wrapper expandOnHover={showExpandAll}>
            <Button
                onClick={(e) => onClickPreventSelect(e) && handleExpandOneLevel(e)}
                onMouseEnter={(e) => e.stopPropagation()}
                onMouseLeave={(e) => e.stopPropagation()}
            >
                <CountWrapper direction={direction}>{count}</CountWrapper>
                <Icon icon="CaretRight" source="phosphor" size="lg" />
            </Button>
            {showExpandAll && (
                <Button
                    onClick={(e) => onClickPreventSelect(e) && handleExpandAll(e)}
                    onMouseEnter={(e) => e.stopPropagation()}
                    onMouseLeave={(e) => e.stopPropagation()}
                >
                    <Icon icon="CaretDoubleRight" source="phosphor" size="lg" />
                </Button>
            )}
        </Wrapper>
    );
}
