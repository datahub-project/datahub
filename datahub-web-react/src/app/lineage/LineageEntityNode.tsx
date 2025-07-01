import { Group } from '@visx/group';
import { LinkHorizontal } from '@visx/shape';
import React, { useContext, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { EntityHealth } from '@app/entity/shared/containers/profile/header/EntityHealth';
import StructuredPropertyBadge, {
    MAX_PROP_BADGE_WIDTH,
} from '@app/entity/shared/containers/profile/header/StructuredPropertyBadge';
import { filterForAssetBadge } from '@app/entity/shared/containers/profile/header/utils';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import LineageEntityColumns from '@app/lineage/LineageEntityColumns';
import {
    centerX,
    centerY,
    healthX,
    healthY,
    iconHeight,
    iconWidth,
    iconX,
    iconY,
    textX,
    width,
} from '@app/lineage/constants';
import ManageLineageMenu from '@app/lineage/manage/ManageLineageMenu';
import { Direction, EntityAndType, EntitySelectParams, UpdatedLineages, VizNode } from '@app/lineage/types';
import { LineageExplorerContext } from '@app/lineage/utils/LineageExplorerContext';
import { convertInputFieldsToSchemaFields } from '@app/lineage/utils/columnLineageUtils';
import { getShortenedTitle, nodeHeightFromTitleLength } from '@app/lineage/utils/titleUtils';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetEntityLineageLazyQuery } from '@graphql/lineage.generated';
import { EntityType } from '@types';

const CLICK_DELAY_THRESHOLD = 1000;
const DRAG_DISTANCE_THRESHOLD = 20;

const PointerGroup = styled(Group)`
    cursor: pointer;
`;

const UnselectableText = styled.text`
    user-select: none;
`;

const MultilineTitleText = styled.p`
    margin-top: -2px;
    font-size: 14px;
    width: 125px;
    word-break: break-all;
`;

const PropertyBadgeWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
`;

export default function LineageEntityNode({
    node,
    isSelected,
    isHovered,
    onEntityClick,
    onEntityCenter,
    onHover,
    onDrag,
    onExpandClick,
    isCenterNode,
    nodesToRenderByUrn,
    setUpdatedLineages,
}: {
    node: VizNode;
    isSelected: boolean;
    isHovered: boolean;
    isCenterNode: boolean;
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onHover: (EntitySelectParams) => void;
    onDrag: (params: EntitySelectParams, event: React.MouseEvent) => void;
    onExpandClick: (data: EntityAndType) => void;
    nodesToRenderByUrn: Record<string, VizNode>;
    setUpdatedLineages: React.Dispatch<React.SetStateAction<UpdatedLineages>>;
}) {
    const { direction } = node;
    const { expandTitles, collapsedColumnsNodes, showColumns, refetchCenterNode } = useContext(LineageExplorerContext);
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const [hasExpanded, setHasExpanded] = useState(false);
    const [isExpanding, setIsExpanding] = useState(false);
    const [expandHover, setExpandHover] = useState(false);
    const [getAsyncEntityLineage, { data: asyncLineageData, loading }] = useGetEntityLineageLazyQuery();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const areColumnsCollapsed = !!collapsedColumnsNodes[node?.data?.urn || 'noop'];
    const isRestricted = node.data.type === EntityType.Restricted;

    function fetchEntityLineage() {
        if (node.data.urn) {
            if (isCenterNode) {
                refetchCenterNode();
            } else {
                // update non-center node using onExpandClick in useEffect below
                getAsyncEntityLineage({
                    variables: {
                        urn: node.data.urn,
                        separateSiblings: isHideSiblingMode,
                        showColumns,
                        startTimeMillis,
                        endTimeMillis,
                    },
                });
                setTimeout(() => setHasExpanded(false), 0);
            }
        }
    }

    const centerEntity = () => {
        if (!isRestricted) {
            onEntityCenter({ urn: node.data.urn, type: node.data.type });
        }
    };

    useEffect(() => {
        if (asyncLineageData && asyncLineageData.entity && !hasExpanded && !loading) {
            const entityAndType = {
                type: asyncLineageData.entity.type,
                entity: { ...asyncLineageData.entity },
            } as EntityAndType;
            onExpandClick(entityAndType);
            setHasExpanded(true);
        }
    }, [asyncLineageData, onExpandClick, hasExpanded, loading]);

    const entityRegistry = useEntityRegistry();
    const unexploredHiddenChildren =
        node?.data?.countercurrentChildrenUrns?.filter((urn) => !(urn in nodesToRenderByUrn))?.length || 0;

    // we need to track lastMouseDownCoordinates to differentiate between clicks and drags. It doesn't use useState because
    // it shouldn't trigger re-renders
    const lastMouseDownCoordinates = useMemo(
        () => ({
            ts: 0,
            x: 0,
            y: 0,
        }),
        [],
    );

    let platformDisplayText =
        node.data.platform?.properties?.displayName || capitalizeFirstLetterOnly(node.data.platform?.name);
    if (node.data.siblingPlatforms && !isHideSiblingMode) {
        platformDisplayText = node.data.siblingPlatforms
            .map((platform) => platform.properties?.displayName || capitalizeFirstLetterOnly(platform.name))
            .join(' & ');
    }

    const nodeHeight = nodeHeightFromTitleLength(
        expandTitles ? node.data.expandedName || node.data.name : undefined,
        node.data.schemaMetadata?.fields || convertInputFieldsToSchemaFields(node.data.inputFields),
        showColumns,
        areColumnsCollapsed,
    );

    const entityName =
        capitalizeFirstLetterOnly(node.data.subtype) ||
        (node.data.type && entityRegistry.getEntityName(node.data.type));

    // Health
    const { health } = node.data;
    const baseUrl = node.data.type && node.data.urn && entityRegistry.getEntityUrl(node.data.type, node.data.urn);
    const hasHealth = (health && baseUrl) || false;

    const entityStructuredProps = node.data.structuredProperties;
    const hasAssetBadge = entityStructuredProps?.properties?.find(filterForAssetBadge);
    const siblingStructuredProps = node.data.siblingStructuredProperties;
    const siblingHasAssetBadge = siblingStructuredProps?.properties?.find(filterForAssetBadge);

    return (
        <PointerGroup data-testid={`node-${node.data.urn}-${direction}`} top={node.x} left={node.y}>
            {unexploredHiddenChildren && (isHovered || isSelected) ? (
                <Group>
                    {[...Array(unexploredHiddenChildren)].map((_, index) => {
                        const link = {
                            source: {
                                x: 0,
                                y: direction === Direction.Upstream ? 70 : -70,
                            },
                            target: {
                                x: (0.5 / (index + 1)) * 80 * (index % 2 === 0 ? 1 : -1),
                                y: direction === Direction.Upstream ? 150 : -150,
                            },
                        };
                        return (
                            <LinkHorizontal
                                // eslint-disable-next-line  react/no-array-index-key
                                key={`link-${index}-${direction}`}
                                data={link}
                                stroke={`url(#gradient-${direction})`}
                                strokeWidth="1"
                                fill="none"
                            />
                        );
                    })}
                </Group>
            ) : null}
            {node.data.unexploredChildren &&
                (!isExpanding ? (
                    <Group
                        onClick={() => {
                            setIsExpanding(true);
                            if (node.data.urn && node.data.type) {
                                // getAsyncEntity(node.data.urn, node.data.type);
                                getAsyncEntityLineage({
                                    variables: {
                                        urn: node.data.urn,
                                        separateSiblings: isHideSiblingMode,
                                        showColumns,
                                        startTimeMillis,
                                        endTimeMillis,
                                    },
                                });
                            }
                        }}
                        onMouseOver={() => {
                            setExpandHover(true);
                        }}
                        onMouseOut={() => {
                            setExpandHover(false);
                        }}
                        pointerEvents="bounding-box"
                    >
                        <circle
                            fill="none"
                            cy={centerY + nodeHeight / 2}
                            cx={direction === Direction.Upstream ? centerX - 10 : centerX + width + 10}
                            r="20"
                        />
                        <circle
                            fill="none"
                            cy={centerY + nodeHeight / 2}
                            cx={direction === Direction.Upstream ? centerX - 30 : centerX + width + 30}
                            r="30"
                        />
                        <g
                            fill={expandHover ? ANTD_GRAY[5] : ANTD_GRAY[6]}
                            transform={`translate(${
                                direction === Direction.Upstream ? centerX - 52 : width / 2 + 10
                            } -21.5) scale(0.04 0.04)`}
                        >
                            <path d="M512 64C264.6 64 64 264.6 64 512s200.6 448 448 448 448-200.6 448-448S759.4 64 512 64zm192 472c0 4.4-3.6 8-8 8H544v152c0 4.4-3.6 8-8 8h-48c-4.4 0-8-3.6-8-8V544H328c-4.4 0-8-3.6-8-8v-48c0-4.4 3.6-8 8-8h152V328c0-4.4 3.6-8 8-8h48c4.4 0 8 3.6 8 8v152h152c4.4 0 8 3.6 8 8v48z" />
                        </g>
                    </Group>
                ) : (
                    <g
                        fill={ANTD_GRAY[6]}
                        transform={`translate(${
                            direction === Direction.Upstream ? centerX - 52 : width / 2 + 10
                        } -21.5) scale(0.04 0.04)`}
                    >
                        <path
                            className="lineageExpandLoading"
                            d="M512 1024c-69.1 0-136.2-13.5-199.3-40.2C251.7 958 197 921 150 874c-47-47-84-101.7-109.8-162.7C13.5 648.2 0 581.1 0 512c0-19.9 16.1-36 36-36s36 16.1 36 36c0 59.4 11.6 117 34.6 171.3 22.2 52.4 53.9 99.5 94.3 139.9 40.4 40.4 87.5 72.2 139.9 94.3C395 940.4 452.6 952 512 952c59.4 0 117-11.6 171.3-34.6 52.4-22.2 99.5-53.9 139.9-94.3 40.4-40.4 72.2-87.5 94.3-139.9C940.4 629 952 571.4 952 512c0-59.4-11.6-117-34.6-171.3a440.45 440.45 0 00-94.3-139.9 437.71 437.71 0 00-139.9-94.3C629 83.6 571.4 72 512 72c-19.9 0-36-16.1-36-36s16.1-36 36-36c69.1 0 136.2 13.5 199.3 40.2C772.3 66 827 103 874 150c47 47 83.9 101.8 109.7 162.7 26.7 63.1 40.2 130.2 40.2 199.3s-13.5 136.2-40.2 199.3C958 772.3 921 827 874 874c-47 47-101.8 83.9-162.7 109.7-63.1 26.8-130.2 40.3-199.3 40.3z"
                        />
                    </g>
                ))}
            <Group
                onDoubleClick={centerEntity}
                onClick={(event) => {
                    if (
                        event.timeStamp < lastMouseDownCoordinates.ts + CLICK_DELAY_THRESHOLD &&
                        Math.sqrt(
                            (event.clientX - lastMouseDownCoordinates.x) ** 2 +
                                (event.clientY - lastMouseDownCoordinates.y) ** 2,
                        ) < DRAG_DISTANCE_THRESHOLD
                    ) {
                        onEntityClick({ urn: node.data.urn, type: node.data.type });
                    }
                }}
                onMouseOver={() => {
                    onHover({ urn: node.data.urn, type: node.data.type });
                }}
                onMouseOut={() => {
                    onHover(undefined);
                }}
                onMouseDown={(event) => {
                    lastMouseDownCoordinates.ts = event.timeStamp;
                    lastMouseDownCoordinates.x = event.clientX;
                    lastMouseDownCoordinates.y = event.clientY;
                    if (node.data.urn && node.data.type) {
                        onDrag({ urn: node.data.urn, type: node.data.type }, event);
                    }
                }}
            >
                <rect
                    height={nodeHeight}
                    width={width}
                    y={centerY}
                    x={centerX}
                    fill="white"
                    stroke={
                        // eslint-disable-next-line no-nested-ternary
                        isSelected ? '#1890FF' : isHovered ? '#1890FF' : 'rgba(192, 190, 190, 0.25)'
                    }
                    strokeWidth={isCenterNode ? 2 : 1}
                    strokeOpacity={1}
                    rx={5}
                    // eslint-disable-next-line react/style-prop-object
                    style={{ filter: isSelected ? 'url(#shadow1-selected)' : 'url(#shadow1)' }}
                />
                {node.data.siblingPlatforms && !isHideSiblingMode && (
                    <svg x={iconX} y={iconY - 5}>
                        <image
                            // preserveAspectRatio="none"
                            y={0}
                            height={iconHeight * (3 / 4)}
                            width={iconWidth * (3 / 4)}
                            href={node.data.siblingPlatforms[0]?.properties?.logoUrl || ''}
                            clipPath="url(#clipPolygonTop)"
                        />
                        <image
                            // preserveAspectRatio="none"
                            y={25}
                            height={iconHeight * (3 / 4)}
                            width={iconWidth * (3 / 4)}
                            clipPath="url(#clipPolygon)"
                            href={node.data.siblingPlatforms[1]?.properties?.logoUrl || ''}
                        />
                    </svg>
                )}
                {(!node.data.siblingPlatforms || isHideSiblingMode) && node.data.icon && (
                    <image href={node.data.icon} height={iconHeight} width={iconWidth} x={iconX} y={iconY} />
                )}
                {!node.data.icon && (!node.data.siblingPlatforms || isHideSiblingMode) && node.data.type && (
                    <svg
                        viewBox="64 64 896 896"
                        focusable="false"
                        x={iconX}
                        y={iconY}
                        height={iconHeight}
                        width={iconWidth}
                        fill="currentColor"
                        aria-hidden="true"
                    >
                        {entityRegistry.getIcon(node.data.type, 16, IconStyleType.SVG)}
                    </svg>
                )}
                {!isRestricted && (
                    <foreignObject
                        x={-centerX - 25}
                        y={centerY + 20}
                        width={20}
                        height={20}
                        onClick={(e) => e.stopPropagation()}
                    >
                        <ManageLineageMenu
                            entityUrn={node.data.urn || ''}
                            refetchEntity={fetchEntityLineage}
                            setUpdatedLineages={setUpdatedLineages}
                            disableUpstream={!isCenterNode && direction === Direction.Downstream}
                            disableDownstream={!isCenterNode && direction === Direction.Upstream}
                            centerEntity={() => onEntityCenter({ urn: node.data.urn, type: node.data.type })}
                            entityType={node.data.type}
                            entityPlatform={node.data.platform?.name}
                            canEditLineage={node.data.canEditLineage}
                        />
                    </foreignObject>
                )}
                {hasAssetBadge && (
                    <foreignObject
                        x={-centerX - MAX_PROP_BADGE_WIDTH - 8}
                        y={centerY - 15}
                        width={MAX_PROP_BADGE_WIDTH}
                        height={30}
                        onClick={(e) => e.stopPropagation()}
                    >
                        <PropertyBadgeWrapper>
                            <StructuredPropertyBadge structuredProperties={entityStructuredProps ?? undefined} />
                        </PropertyBadgeWrapper>
                    </foreignObject>
                )}
                {!hasAssetBadge && siblingHasAssetBadge && (
                    <foreignObject
                        x={-centerX - MAX_PROP_BADGE_WIDTH - 8}
                        y={centerY - 15}
                        width={MAX_PROP_BADGE_WIDTH}
                        height={30}
                        onClick={(e) => e.stopPropagation()}
                    >
                        <PropertyBadgeWrapper>
                            <StructuredPropertyBadge structuredProperties={siblingStructuredProps ?? undefined} />
                        </PropertyBadgeWrapper>
                    </foreignObject>
                )}
                <Group>
                    <UnselectableText
                        dy="-1em"
                        x={textX}
                        fontSize={8}
                        fontFamily="Manrope"
                        fontWeight="bold"
                        textAnchor="start"
                        fill="#8C8C8C"
                    >
                        {platformDisplayText && (
                            <>
                                <tspan>{getShortenedTitle(platformDisplayText || '', width)}</tspan>
                                <tspan dx=".25em" dy="2px" fill="#dadada" fontSize={12} fontWeight="normal">
                                    {' '}
                                    |{' '}
                                </tspan>
                            </>
                        )}
                        <tspan dx=".25em" dy="-2px" data-testid={entityName}>
                            {entityName}
                        </tspan>
                    </UnselectableText>
                    {expandTitles ? (
                        <foreignObject x={textX} width="125" height="200">
                            <MultilineTitleText>{node.data.expandedName || node.data.name}</MultilineTitleText>
                        </foreignObject>
                    ) : (
                        <UnselectableText
                            dy="1em"
                            x={textX}
                            fontSize={14}
                            fontFamily="Manrope"
                            textAnchor="start"
                            fill={isCenterNode ? '#1890FF' : 'black'}
                        >
                            {getShortenedTitle(node.data.name, width)}
                        </UnselectableText>
                    )}
                    <foreignObject x={healthX} y={healthY} width="20" height="20">
                        {hasHealth && (
                            <EntityHealth
                                health={health as any}
                                baseUrl={baseUrl as any}
                                fontSize={20}
                                tooltipPlacement="top"
                            />
                        )}
                    </foreignObject>
                </Group>
                {unexploredHiddenChildren && isHovered ? (
                    <UnselectableText
                        dy=".33em"
                        dx={textX}
                        fontSize={16}
                        fontFamily="Arial"
                        textAnchor="middle"
                        fill="black"
                        y={centerY - 20}
                    >
                        {unexploredHiddenChildren} hidden {direction === Direction.Upstream ? 'downstream' : 'upstream'}{' '}
                        {unexploredHiddenChildren > 1 ? 'dependencies' : 'dependency'}
                    </UnselectableText>
                ) : null}
                {showColumns && (node.data.schemaMetadata || node.data.inputFields) && (
                    <LineageEntityColumns node={node} onHover={onHover} />
                )}
            </Group>
        </PointerGroup>
    );
}
