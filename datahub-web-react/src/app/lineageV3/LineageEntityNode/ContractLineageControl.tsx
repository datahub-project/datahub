import { Icon } from '@components';
import { CaretLeft } from '@phosphor-icons/react/dist/csr/CaretLeft';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useContext, useState } from 'react';
import { Trans } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { ContractLineageButton } from '@app/lineageV3/LineageEntityNode/ContractLineageButton';
import { Button } from '@app/lineageV3/LineageEntityNode/components';
import LineageFilterSearch from '@app/lineageV3/LineageFilterNode/LineageFilterSearch';
import LineageFilterSummary from '@app/lineageV3/LineageFilterNode/LineageFilterSummary';
import { ShowMoreButton } from '@app/lineageV3/LineageFilterNode/ShowMoreButton';
import useFetchFilterNodeContents from '@app/lineageV3/LineageFilterNode/useFetchFilterNodeContents';
import {
    LineageDisplayContext,
    LineageFilter,
    LineageNodesContext,
    createLineageFilterNodeId,
    onClickPreventSelect,
} from '@app/lineageV3/common';

import { LineageDirection } from '@types';

const ControlWrapper = styled.div<{ direction: LineageDirection }>`
    position: absolute;
    ${({ direction }) =>
        direction === LineageDirection.Upstream ? 'right: calc(100% + 10px);' : 'left: calc(100% + 10px);'}
    transform: translateY(-50%);

    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 4px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    color: ${(props) => props.theme.colors.iconBrand};
    cursor: pointer;
    font-size: 18px;

    display: flex;
    align-items: center;
`;

const CountText = styled.span`
    font-size: 12px;
    font-weight: 600;
    white-space: nowrap;
`;

const Panel = styled.div<{ direction: LineageDirection; visible: boolean }>`
    position: absolute;
    bottom: calc(100% + 6px);
    ${({ direction }) => (direction === LineageDirection.Upstream ? 'right: 0;' : 'left: 0;')}

    background-color: ${(props) => props.theme.colors.bg};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    color: ${(props) => props.theme.colors.text};
    cursor: default;
    font-size: 12px;
    line-height: 16px;

    display: flex;
    flex-direction: column;
    gap: 4px;
    padding: 8px;
    width: 250px;

    opacity: ${({ visible }) => (visible ? 1 : 0)};
    pointer-events: ${({ visible }) => (visible ? 'all' : 'none')};
    transform: translateY(${({ visible }) => (visible ? '0' : '6px')});
    transition:
        opacity 0.2s ease-in-out,
        transform 0.2s ease-in-out;

    /* Invisible bridge over the gap to the button, so the panel stays open while crossing it */
    ::after {
        content: '';
        position: absolute;
        top: 100%;
        left: 0;
        right: 0;
        height: 12px;
    }
`;

const PanelHeader = styled.div`
    align-items: center;
    display: flex;
    gap: 8px;
    justify-content: space-between;
`;

const TitleCount = styled.span`
    font-weight: 600;
`;

interface Props {
    urn: string;
    direction: LineageDirection;
}

/**
 * Contract button for an expanded node. If some of the node's children are not displayed
 * (`lineageFilters` has an entry for this node and direction), makes that clear by showing how
 * many children are shown, and expands on hover into a control to contract, search, and paginate
 * children. Used in place of lineage filter nodes when those are not rendered.
 */
export function ContractLineageControl({ urn, direction }: Props) {
    const { lineageFilters } = useContext(LineageDisplayContext);
    const filter = lineageFilters.get(createLineageFilterNodeId(urn, direction));

    if (!filter) {
        return <ContractLineageButton urn={urn} direction={direction} />;
    }
    return <FilteredChildrenControl urn={urn} direction={direction} filter={filter} />;
}

function FilteredChildrenControl({ urn, direction, filter }: Props & { filter: LineageFilter }) {
    const { nodes, setDisplayVersion, showGhostEntities } = useContext(LineageNodesContext);
    const [isHovered, setIsHovered] = useState(false);
    // Panel contents are mounted on first hover and kept mounted, so search state persists
    const [hasHovered, setHasHovered] = useState(false);
    const [numMatches, setNumMatches] = useState(0);

    const { total, platforms, subtypes } = useFetchFilterNodeContents(urn, direction, !hasHovered);
    const denominator = showGhostEntities ? filter.allChildren.size : (total ?? filter.allChildren.size);
    const shown = Math.min(filter.shown.size, denominator);

    const contractLineage = (e: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
        e.stopPropagation();
        const node = nodes.get(urn);
        if (node) {
            node.isExpanded = { ...node.isExpanded, [direction]: false };
            // Keeping the previous zoom node list leaves the viewport in place
            setDisplayVersion(([version, n]) => [version + 1, n]);
            analytics.event({
                type: EventType.ContractLineageEvent,
                direction,
                entityUrn: urn,
                entityType: node.type,
            });
        }
    };

    const contractIcon = direction === LineageDirection.Upstream ? CaretRight : CaretLeft;
    return (
        <ControlWrapper
            className="nodrag"
            direction={direction}
            data-testid={`contract-lineage-control-${urn}-${direction}`}
            onMouseEnter={(e) => {
                e.stopPropagation();
                setIsHovered(true);
                setHasHovered(true);
            }}
            onMouseLeave={(e) => {
                e.stopPropagation();
                setIsHovered(false);
            }}
        >
            <Panel visible={isHovered} direction={direction} onClick={onClickPreventSelect}>
                {hasHovered && (
                    <>
                        <PanelHeader>
                            <span>
                                <Trans
                                    i18nKey="lineage:filter.shownOfTotal"
                                    values={{ shown, total: denominator, plus: showGhostEntities ? '+' : '' }}
                                    components={{ shown: <TitleCount />, total: <TitleCount /> }}
                                />
                            </span>
                            <ShowMoreButton data={filter} numMatches={numMatches} />
                        </PanelHeader>
                        <LineageFilterSearch
                            data={filter}
                            numMatches={numMatches}
                            setNumMatches={setNumMatches}
                            zoomToNode={false}
                        />
                        <LineageFilterSummary platforms={platforms} subtypes={subtypes} />
                    </>
                )}
            </Panel>
            <Button
                onClick={(e) => onClickPreventSelect(e) && contractLineage(e)}
                data-testid={`contract-${urn}-button`}
            >
                <CountText data-testid={`children-shown-${urn}-${direction}`}>
                    {shown}/{denominator}
                </CountText>
                <Icon icon={contractIcon} size="lg" />
            </Button>
        </ControlWrapper>
    );
}
