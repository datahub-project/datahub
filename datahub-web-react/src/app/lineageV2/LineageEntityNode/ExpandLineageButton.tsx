import React from 'react';
import styled from 'styled-components';
import { KeyboardArrowRight, KeyboardDoubleArrowRight } from '@mui/icons-material';
import { EntityType, LineageDirection } from '../../../types.generated';
import { FetchStatus, onMouseDownCapturePreventSelect } from '../common';
import { ANTD_GRAY } from '../../entityV2/shared/constants';
import { UpstreamWrapper, DownstreamWrapper, Button } from './components';
import { useOnClickExpandLineage } from './useOnClickExpandLineage';
import analytics, { EventType } from '../../analytics';

const VerticalDivider = styled.hr<{ margin: number }>`
    align-self: stretch;
    height: auto;
    margin: 0 ${({ margin }) => margin}px;
    border: 0.5px solid ${ANTD_GRAY[5]};
    vertical-align: text-top;
`;

interface Props {
    urn: string;
    direction: LineageDirection;
    display: boolean;
    entityType: EntityType;
    fetchStatus: Record<LineageDirection, FetchStatus>;
}

export function ExpandLineageButton({ urn, direction, display, entityType, fetchStatus }: Props) {
    const expandOneLevel = useOnClickExpandLineage(urn, direction, false);
    const expandAll = useOnClickExpandLineage(urn, direction, true);
    const isFetchComplete = fetchStatus[direction] === FetchStatus.COMPLETE;

    const handleExpandOneLevel = (e: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
        expandOneLevel(e);
        analytics.event({
            type: EventType.ExpandLineageEvent,
            direction,
            entityUrn: urn,
            entityType,
            levelsExpanded: '1',
        });
    };

    const handleExpandAll = (e: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
        expandAll(e);
        analytics.event({
            type: EventType.ExpandLineageEvent,
            direction,
            entityUrn: urn,
            entityType,
            levelsExpanded: 'all',
        });
    };

    // Still have to render this component while request is loading, otherwise it gets cancelled
    // But don't render the buttons while the request is in progress
    // TODO: Reset fetch status if the request fails
    if (!display) return null;

    const Wrapper = direction === LineageDirection.Upstream ? UpstreamWrapper : DownstreamWrapper;

    return (
        <Wrapper expandOnHover={!isFetchComplete}>
            <Button
                onClick={handleExpandOneLevel}
                onMouseDownCapture={onMouseDownCapturePreventSelect}
                onMouseEnter={(e) => e.stopPropagation()}
                onMouseLeave={(e) => e.stopPropagation()}
            >
                <KeyboardArrowRight viewBox="3 3 18 18" fontSize="inherit" />
            </Button>
            {!isFetchComplete && (
                <>
                    <VerticalDivider margin={2} />
                    <Button
                        onClick={handleExpandAll}
                        onMouseDownCapture={onMouseDownCapturePreventSelect}
                        onMouseEnter={(e) => e.stopPropagation()}
                        onMouseLeave={(e) => e.stopPropagation()}
                    >
                        <KeyboardDoubleArrowRight viewBox="3 3 18 18" fontSize="inherit" />
                    </Button>
                </>
            )}
        </Wrapper>
    );
}
