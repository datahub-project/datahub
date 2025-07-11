import { Icon } from '@components';
import React, { useContext } from 'react';

import analytics, { EventType } from '@app/analytics';
import { Button, DownstreamWrapper, UpstreamWrapper } from '@app/lineageV3/LineageEntityNode/components';
import { LineageNodesContext, onClickPreventSelect } from '@app/lineageV3/common';

import { LineageDirection } from '@types';

interface Props {
    urn: string;
    direction: LineageDirection;
}

export function ContractLineageButton({ urn, direction }: Props) {
    const context = useContext(LineageNodesContext);
    const { nodes, setDisplayVersion } = context;

    const contractLineage = (e: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
        e.stopPropagation();
        const node = nodes.get(urn);
        if (node) {
            node.isExpanded = { ...node.isExpanded, [direction]: false };
            setDisplayVersion(([version, n]) => [version + 1, n]);
            analytics.event({
                type: EventType.ContractLineageEvent,
                direction,
                entityUrn: urn,
                entityType: node.type,
            });
        }
    };

    const Wrapper = direction === LineageDirection.Upstream ? UpstreamWrapper : DownstreamWrapper;

    return (
        <Wrapper>
            <Button
                onClick={(e) => onClickPreventSelect(e) && contractLineage(e)}
                onMouseEnter={(e) => e.stopPropagation()}
                onMouseLeave={(e) => e.stopPropagation()}
            >
                <Icon icon="CaretLeft" source="phosphor" size="lg" />
            </Button>
        </Wrapper>
    );
}
