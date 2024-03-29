import React, { useContext } from 'react';
import styled from 'styled-components';
import { KeyboardArrowRight, KeyboardDoubleArrowRight } from '@mui/icons-material';
import { LineageDirection } from '../../../types.generated';
import { FetchStatus, LineageNodesContext, onMouseDownCapturePreventSelect } from '../common';
import useSearchAcrossLineage from '../useSearchAcrossLineage';
import { ANTD_GRAY, LINEAGE_COLORS } from '../../entityV2/shared/constants';

const ExpandButton = styled.div`
    background-color: white;
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 10px;
    color: ${LINEAGE_COLORS.BLUE_1};
    cursor: pointer;
    display: flex;
    font-size: 18px;
    padding: 3px;
    position: absolute;
    top: 50%;

    max-width: 25px;
    overflow: hidden;
    transition: max-width 0.3s ease-in-out;

    :hover {
        max-width: 50px;
    }
`;

const UpstreamWrapper = styled(ExpandButton)`
    right: calc(100% - 5px);
    transform: translateY(-50%) rotate(180deg);
`;

const DownstreamWrapper = styled(ExpandButton)`
    left: calc(100% - 5px);
    transform: translateY(-50%);
`;

const VerticalDivider = styled.hr<{ margin: number }>`
    align-self: stretch;
    height: auto;
    margin: 0 ${({ margin }) => margin}px;
    border: 0.5px solid ${ANTD_GRAY[5]};
    vertical-align: text-top;
`;

const Button = styled.span`
    border-radius: 20%;
    line-height: 0;

    :hover {
        background-color: ${LINEAGE_COLORS.BLUE_1}30;
    }
`;

interface Props {
    urn: string;
    direction: LineageDirection;
    display: boolean;
}

export function ExpandLineageButton({ urn, direction, display }: Props) {
    const expandOneLevel = useOnClickExpandLineage(urn, direction, false);
    const expandAll = useOnClickExpandLineage(urn, direction, true);

    // Still have to render this component while request is loading, otherwise it gets cancelled
    // But don't render the buttons while the request is in progress
    // TODO: Reset fetch status if the request fails
    if (!display) return null;

    const Wrapper = direction === LineageDirection.Upstream ? UpstreamWrapper : DownstreamWrapper;

    return (
        <Wrapper>
            <Button
                onClick={expandOneLevel}
                onMouseDownCapture={onMouseDownCapturePreventSelect}
                onMouseEnter={(e) => e.stopPropagation()}
                onMouseLeave={(e) => e.stopPropagation()}
            >
                <KeyboardArrowRight viewBox="3 3 18 18" fontSize="inherit" />
            </Button>
            <VerticalDivider margin={2} />
            <Button
                onClick={expandAll}
                onMouseDownCapture={onMouseDownCapturePreventSelect}
                onMouseEnter={(e) => e.stopPropagation()}
                onMouseLeave={(e) => e.stopPropagation()}
            >
                <KeyboardDoubleArrowRight viewBox="3 3 18 18" fontSize="inherit" />
            </Button>
        </Wrapper>
    );
}

function useOnClickExpandLineage(urn: string, direction: LineageDirection, maxDepth: boolean) {
    const context = useContext(LineageNodesContext);
    const { nodes, setDataVersion } = context;
    const { fetchLineage } = useSearchAcrossLineage(urn, context, direction, true, maxDepth);

    return function onClick(e: React.MouseEvent<HTMLDivElement, MouseEvent>) {
        e.stopPropagation();
        const node = nodes.get(urn);
        const fetchStatus = node?.fetchStatus?.[direction];
        if (node && fetchStatus !== FetchStatus.COMPLETE) {
            node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.LOADING };
            node.isExpanded = true;
            setDataVersion((v) => v + 1);
            fetchLineage();
        }
    };
}
