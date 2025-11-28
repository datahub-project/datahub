import React, { useRef, useEffect, useState } from "react";
import styles from "./styles.module.css";
import DataHubLineageNode from "../DataHubLineageNode";

const LineageLayoutGrid = ({
  title,
  layers = [],
  showConnections = true,
  allExpanded = false,
  onToggleExpand = () => {},
  connectionColors = {},
  defaultColors = ["#533FD1", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6"],
}) => {
  const containerRef = useRef(null);
  const [connections, setConnections] = useState([]);

  // Build a map of all nodes with their positions (supports nested sub-layers)
  const buildNodeMap = () => {
    const nodeMap = new Map();

    const addNodesFromLayer = (layer, layerIndex) => {
      // Add standalone nodes if present
      if (Array.isArray(layer.nodes)) {
        layer.nodes.forEach((node, nodeIndex) => {
          const element = containerRef.current.querySelector(
            `[data-node-id="${node.name}"]`,
          );
          if (element) {
            const containerRect = containerRef.current.getBoundingClientRect();
            const nodeElement = element.firstElementChild || element;
            const nodeRect = nodeElement.getBoundingClientRect();
            const containerScrollLeft = containerRef.current.scrollLeft;
            const containerScrollTop = containerRef.current.scrollTop;

            nodeMap.set(node.name, {
              node,
              layerIndex,
              nodeIndex,
              x: nodeRect.left - containerRect.left + containerScrollLeft,
              y: nodeRect.top - containerRect.top + containerScrollTop,
              width: nodeRect.width,
              height: nodeRect.height,
              centerX:
                nodeRect.left -
                containerRect.left +
                nodeRect.width / 2 +
                containerScrollLeft,
              centerY:
                nodeRect.top -
                containerRect.top +
                nodeRect.height / 2 +
                containerScrollTop,
              rightEdge:
                nodeRect.right - containerRect.left + containerScrollLeft,
              leftEdge:
                nodeRect.left - containerRect.left + containerScrollLeft,
            });
          }
        });
      }

      // Recurse into sub-layers if present
      if (Array.isArray(layer.subLayers)) {
        layer.subLayers.forEach((subLayer) =>
          addNodesFromLayer(subLayer, layerIndex),
        );
      }
    };

    layers.forEach((layer, layerIndex) => addNodesFromLayer(layer, layerIndex));

    return nodeMap;
  };

  // Calculate connections with proper routing around nodes
  const calculateConnections = () => {
    if (!containerRef.current || !showConnections) return;

    const nodeMap = buildNodeMap();
    const newConnections = [];

    // Find all connections across all layers (including nested sub-layers)
    const processNodes = (nodes) => {
      nodes.forEach((sourceNode, sourceIndex) => {
        if (!sourceNode.downstreamConnections) return;

        sourceNode.downstreamConnections.forEach((targetNodeName) => {
          const sourceNodeData = nodeMap.get(sourceNode.name);
          const targetNodeData = nodeMap.get(targetNodeName);

          if (sourceNodeData && targetNodeData) {
            const connectionColor =
              connectionColors[sourceNode.name] ||
              defaultColors[sourceIndex % defaultColors.length];
            const path = calculateRoutingPath(
              sourceNodeData,
              targetNodeData,
              nodeMap,
            );

            const arrowMarkerWidth = 10; // keep in sync with marker path and viewBox size
            const backoffPx = 10; // small gap to avoid overlapping the node border
            newConnections.push({
              id: `${sourceNode.name}-${targetNodeName}`,
              sourceX: sourceNodeData.rightEdge,
              sourceY: sourceNodeData.centerY,
              // End the path at the center of the back of the arrowhead, with a slight gap before the node.
              targetX: targetNodeData.leftEdge - (arrowMarkerWidth + backoffPx),
              targetY: targetNodeData.centerY,
              color: connectionColor,
              path: path,
              layerIndex: sourceNodeData.layerIndex,
              sourceIndex,
            });
          }
        });
      });
    };

    const traverseForConnections = (layer) => {
      if (Array.isArray(layer.nodes)) processNodes(layer.nodes);
      if (Array.isArray(layer.subLayers))
        layer.subLayers.forEach(traverseForConnections);
    };

    layers.forEach(traverseForConnections);

    setConnections(newConnections);
  };

  // Calculate routing path that avoids nodes with proper collision detection
  const calculateRoutingPath = (sourceData, targetData, nodeMap) => {
    const sourceX = sourceData.rightEdge;
    const sourceY = sourceData.centerY;
    const targetX = targetData.leftEdge - 16;
    const targetY = targetData.centerY;

    // Check if there are nodes between source and target that we need to route around
    const intermediateNodes = Array.from(nodeMap.values()).filter(
      (nodeData) => {
        // Add buffer zones around nodes to ensure we don't clip them
        const nodeBuffer = 25; // Extra space around nodes
        const nodeLeft = nodeData.x - nodeBuffer;
        const nodeRight = nodeData.x + nodeData.width + nodeBuffer;
        const nodeTop = nodeData.y - nodeBuffer;
        const nodeBottom = nodeData.y + nodeData.height + nodeBuffer;

        // Check if this node is in the horizontal path between source and target
        const isInHorizontalPath = nodeLeft < targetX && nodeRight > sourceX;
        const isNotSourceOrTarget =
          nodeData.node.name !== sourceData.node.name &&
          nodeData.node.name !== targetData.node.name;

        // Also check if the direct line from source to target would intersect this node
        const directLineIntersectsNode =
          // Line passes through the node's Y range
          (sourceY <= nodeBottom && sourceY >= nodeTop) ||
          (targetY <= nodeBottom && targetY >= nodeTop) ||
          (sourceY <= nodeTop && targetY >= nodeBottom) ||
          (sourceY >= nodeBottom && targetY <= nodeTop);

        return (
          isInHorizontalPath && isNotSourceOrTarget && directLineIntersectsNode
        );
      },
    );

    if (intermediateNodes.length === 0) {
      // Direct path if no obstacles
      return null; // Will use default curve
    }

    // Calculate routing paths that avoid all intermediate nodes
    const nodeObstacles = intermediateNodes.map((nodeData) => ({
      top: nodeData.y - 25, // Buffer above node
      bottom: nodeData.y + nodeData.height + 25, // Buffer below node
      left: nodeData.x - 25,
      right: nodeData.x + nodeData.width + 25,
      centerY: nodeData.centerY,
      name: nodeData.node.name,
    }));

    // Find the best routing level (above or below obstacles)
    const allTops = nodeObstacles.map((n) => n.top);
    const allBottoms = nodeObstacles.map((n) => n.bottom);

    const highestTop = Math.min(...allTops);
    const lowestBottom = Math.max(...allBottoms);

    // Calculate routing options with more clearance
    const routingOffset = 60; // Larger offset to more clearly bend around nodes
    const routeAbove = highestTop - routingOffset;
    const routeBelow = lowestBottom + routingOffset;

    // Choose the route that's closer to the average of source and target Y positions
    const avgY = (sourceY + targetY) / 2;
    const routingY =
      Math.abs(routeAbove - avgY) < Math.abs(routeBelow - avgY)
        ? routeAbove
        : routeBelow;

    return {
      type: "routed",
      routingY,
      obstacles: nodeObstacles,
    };
  };

  // Recalculate connections when layout changes
  useEffect(() => {
    const timer = setTimeout(calculateConnections, 100); // Small delay for DOM updates
    return () => clearTimeout(timer);
  }, [layers, allExpanded]);

  // Recalculate on window resize and scroll
  useEffect(() => {
    const handleResize = () => calculateConnections();
    const handleScroll = () => calculateConnections();

    window.addEventListener("resize", handleResize);

    // Add scroll listener to the container
    if (containerRef.current) {
      containerRef.current.addEventListener("scroll", handleScroll);
    }

    return () => {
      window.removeEventListener("resize", handleResize);
      if (containerRef.current) {
        containerRef.current.removeEventListener("scroll", handleScroll);
      }
    };
  }, []);

  // Generate path that routes around nodes when needed
  const generatePath = (connection) => {
    const { sourceX, sourceY, targetX, targetY, path } = connection;

    if (!path || path.type !== "routed") {
      // Simple Bezier curve for direct connections
      const horizontalDistance = targetX - sourceX;
      const cp1X = sourceX + horizontalDistance * 0.5;
      const cp1Y = sourceY;
      const cp2X = sourceX + horizontalDistance * 0.5;
      const cp2Y = targetY;

      return `M ${sourceX} ${sourceY} C ${cp1X} ${cp1Y}, ${cp2X} ${cp2Y}, ${targetX} ${targetY}`;
    }

    // Routed path using two cubic segments that pass through a safe routing level
    const { routingY } = path;
    const midX = sourceX + (targetX - sourceX) * 0.5;
    const bend = 40; // Horizontal control point offset for smooth bends

    // First curve: from source to mid point at routingY
    const cp1X = sourceX + bend;
    const cp1Y = sourceY;
    const cp2X = midX - bend;
    const cp2Y = routingY;

    // Second curve: from mid point at routingY to target
    const cp3X = midX + bend;
    const cp3Y = routingY;
    const cp4X = targetX - bend;
    const cp4Y = targetY;

    return `M ${sourceX} ${sourceY} C ${cp1X} ${cp1Y}, ${cp2X} ${cp2Y}, ${midX} ${routingY} C ${cp3X} ${cp3Y}, ${cp4X} ${cp4Y}, ${targetX} ${targetY}`;
  };

  // Recursive renderer for layers and sublayers
  const renderLayerContent = (layer) => {
    const hasSubLayers =
      Array.isArray(layer.subLayers) && layer.subLayers.length > 0;

    if (!hasSubLayers) {
      // Render standalone nodes
      return (
        <div className={styles.layerNodes}>
          {(layer.nodes || []).map((node, nodeIndex) => (
            <div
              key={node.name || nodeIndex}
              data-node-id={node.name}
              className={styles.nodeWrapper}
            >
              <DataHubLineageNode
                {...node}
                isExpanded={allExpanded}
                onToggleExpand={onToggleExpand}
              />
            </div>
          ))}
        </div>
      );
    }

    // Render sublayers: support horizontal columns or vertical stacks at each level
    if (layer.subLayersLayout === "columns") {
      return (
        <div className={styles.subLayersRowContainer}>
          {/* Leftmost column for standalone nodes at this level */}
          {Array.isArray(layer.nodes) && layer.nodes.length > 0 && (
            <div className={styles.subLayerColumn}>
              <div className={styles.layerNodesLeft}>
                {layer.nodes.map((node, nodeIndex) => (
                  <div
                    key={node.name || nodeIndex}
                    data-node-id={node.name}
                    className={styles.nodeWrapper}
                  >
                    <DataHubLineageNode
                      {...node}
                      isExpanded={allExpanded}
                      onToggleExpand={onToggleExpand}
                    />
                  </div>
                ))}
              </div>
            </div>
          )}
          {layer.subLayers.map((subLayer, subLayerIndex) => (
            <div
              key={subLayer.name || subLayerIndex}
              className={styles.subLayerColumn}
            >
              {subLayer.title && (
                <div className={styles.subLayerTitle}>{subLayer.title}</div>
              )}
              {renderLayerContent(subLayer)}
            </div>
          ))}
        </div>
      );
    }

    // Default vertical stack
    return (
      <div className={styles.subLayersContainer}>
        {layer.subLayers.map((subLayer, subLayerIndex) => (
          <div key={subLayer.name || subLayerIndex} className={styles.subLayer}>
            {subLayer.title && (
              <div className={styles.subLayerTitle}>{subLayer.title}</div>
            )}
            {renderLayerContent(subLayer)}
          </div>
        ))}
      </div>
    );
  };

  return (
    <div className={styles.lineageContainer} ref={containerRef}>
      {title && <h3 className={styles.title}>{title}</h3>}

      <div className={styles.layersGrid}>
        {layers.map((layer, layerIndex) => (
          <div key={layer.name || layerIndex} className={styles.layer}>
            {layer.title && (
              <div className={styles.layerTitle}>{layer.title}</div>
            )}

            {renderLayerContent(layer)}
          </div>
        ))}
      </div>

      {/* SVG overlay for connections */}
      {showConnections && connections.length > 0 && (
        <svg
          className={styles.connectionsOverlay}
          style={{
            width: "100%",
            height: "100%",
            position: "absolute",
            top: 0,
            left: 0,
            pointerEvents: "none",
          }}
        >
          <defs>
            {connections.map((connection, index) => (
              <marker
                key={`marker-${connection.id}`}
                id={`arrowhead-${connection.id}`}
                markerWidth="10"
                markerHeight="8"
                refX="0" /* anchor the center of the back of the arrow at the path end */
                refY="4"
                orient="0"
                markerUnits="strokeWidth"
              >
                <path
                  d="M 0 0 L 0 8 L 10 4 z"
                  fill={connection.color}
                  opacity="0.9"
                />
              </marker>
            ))}
          </defs>

          {connections.map((connection) => (
            <g key={connection.id}>
              <path
                d={generatePath(connection)}
                stroke={connection.color}
                strokeWidth="2"
                fill="none"
                opacity="0.9"
                markerEnd={`url(#arrowhead-${connection.id})`}
                className={styles.connectionPath}
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <circle
                cx={connection.sourceX}
                cy={connection.sourceY}
                r="2"
                fill={connection.color}
                opacity="1"
              />
              <circle
                cx={connection.targetX}
                cy={connection.targetY}
                r="2"
                fill={connection.color}
                opacity="1"
              />
            </g>
          ))}
        </svg>
      )}
    </div>
  );
};

export default LineageLayoutGrid;
export { LineageLayoutGrid };
