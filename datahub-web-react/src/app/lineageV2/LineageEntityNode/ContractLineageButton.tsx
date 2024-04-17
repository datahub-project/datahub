import React, { useContext } from 'react';
import { KeyboardArrowLeft } from '@mui/icons-material';
import { LineageDirection } from '../../../types.generated';
import { UpstreamWrapper, DownstreamWrapper, Button } from './components';
import { LineageNodesContext, onMouseDownCapturePreventSelect } from '../common';
import analytics, { EventType } from '../../analytics';

interface Props {
    urn: string;
    direction: LineageDirection;
}

export function ContractLineageButton({ urn, direction }: Props) {
    const context = useContext(LineageNodesContext);
    const { nodes, setDisplayVersion } = context;

    const contractLineage = (e: React.MouseEvent<HTMLDivElement, MouseEvent>) => {
        e.stopPropagation();
        const node = nodes.get(urn);
        if (node) {
            node.isExpanded = { ...node.isExpanded, [direction]: false };
            setDisplayVersion(([version]) => [version + 1, []]);
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
                onClick={contractLineage}
                onMouseDownCapture={onMouseDownCapturePreventSelect}
                onMouseEnter={(e) => e.stopPropagation()}
                onMouseLeave={(e) => e.stopPropagation()}
            >
                <KeyboardArrowLeft viewBox="4 3 18 18" fontSize="inherit" />
            </Button>
        </Wrapper>
    );
}
