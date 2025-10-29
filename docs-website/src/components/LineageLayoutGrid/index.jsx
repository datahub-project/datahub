import React, { useRef, useEffect, useState } from 'react';
import styles from './styles.module.css';
import DataHubLineageNode from '../DataHubLineageNode';

const LineageLayoutGrid = ({ 
  title, 
  layers = [], 
  showConnections = true,
  allExpanded = false,
  onToggleExpand = () => {},
  connectionColors = {},
  defaultColors = ['#533FD1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6']
}) => {
  const containerRef = useRef(null);
  const [connections, setConnections] = useState([]);

  // Build a map of all nodes with their positions
  const buildNodeMap = () => {
    const nodeMap = new Map();
    
    layers.forEach((layer, layerIndex) => {
      layer.nodes.forEach((node, nodeIndex) => {
        const element = containerRef.current.querySelector(`[data-node-id="${node.name}"]`);
        if (element) {
          const containerRect = containerRef.current.getBoundingClientRect();
          const nodeRect = element.getBoundingClientRect();
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
            centerX: nodeRect.left - containerRect.left + nodeRect.width / 2 + containerScrollLeft,
            centerY: nodeRect.top - containerRect.top + nodeRect.height / 2 + containerScrollTop,
            rightEdge: nodeRect.right - containerRect.left + containerScrollLeft,
            leftEdge: nodeRect.left - containerRect.left + containerScrollLeft
          });
        }
      });
    });
    
    return nodeMap;
  };

  // Calculate connections with proper routing around nodes
  const calculateConnections = () => {
    if (!containerRef.current || !showConnections) return;

    const nodeMap = buildNodeMap();
    const newConnections = [];

    // Find all connections across all layers
    layers.forEach((layer, layerIndex) => {
      layer.nodes.forEach((sourceNode, sourceIndex) => {
        if (!sourceNode.downstreamConnections) return;

        sourceNode.downstreamConnections.forEach(targetNodeName => {
          const sourceNodeData = nodeMap.get(sourceNode.name);
          const targetNodeData = nodeMap.get(targetNodeName);
          
          if (sourceNodeData && targetNodeData) {
            // Get connection color from props or use default
            const connectionColor = connectionColors[sourceNode.name] || defaultColors[sourceIndex % defaultColors.length];

            // Calculate routing path that avoids intermediate nodes
            const path = calculateRoutingPath(sourceNodeData, targetNodeData, nodeMap);

            // Debug: Log DataJob connections specifically
            if (sourceNode.entityType === 'DataJob' || targetNodeData.node.entityType === 'DataJob') {
              console.log(`DataJob connection: ${sourceNode.name} (${sourceNode.entityType}) → ${targetNodeName} (${targetNodeData.node.entityType})`);
            }

            newConnections.push({
              id: `${sourceNode.name}-${targetNodeName}`,
              sourceX: sourceNodeData.rightEdge,
              sourceY: sourceNodeData.centerY,
              targetX: targetNodeData.leftEdge - 16, // Account for arrowhead
              targetY: targetNodeData.centerY,
              color: connectionColor,
              path: path,
              layerIndex: sourceNodeData.layerIndex,
              sourceIndex
            });
          }
        });
      });
    });

    setConnections(newConnections);
  };

  // Calculate routing path that avoids nodes
  const calculateRoutingPath = (sourceData, targetData, nodeMap) => {
    const sourceX = sourceData.rightEdge;
    const sourceY = sourceData.centerY;
    const targetX = targetData.leftEdge - 16;
    const targetY = targetData.centerY;

    // Check if there are nodes between source and target that we need to route around
    const intermediateNodes = Array.from(nodeMap.values()).filter(nodeData => {
      // Only consider nodes that are between source and target horizontally
      return nodeData.centerX > sourceX && nodeData.centerX < targetX &&
             nodeData.node.name !== sourceData.node.name && 
             nodeData.node.name !== targetData.node.name;
    });

    if (intermediateNodes.length === 0) {
      // Direct path if no obstacles
      return null; // Will use default curve
    }

    // Find routing level that avoids all intermediate nodes
    const allNodeYs = intermediateNodes.map(n => [n.y, n.y + n.height]).flat();
    allNodeYs.push(sourceY, targetY);
    
    const minY = Math.min(...allNodeYs);
    const maxY = Math.max(...allNodeYs);
    
    // Route above or below based on which has more space and is more natural
    const routingOffset = 40;
    const routeAbove = minY - routingOffset;
    const routeBelow = maxY + routingOffset;
    
    // Choose the route that's closer to the average of source and target
    const avgY = (sourceY + targetY) / 2;
    const routingY = Math.abs(routeAbove - avgY) < Math.abs(routeBelow - avgY) ? routeAbove : routeBelow;

    return {
      type: 'routed',
      routingY
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
    
    window.addEventListener('resize', handleResize);
    
    // Add scroll listener to the container
    if (containerRef.current) {
      containerRef.current.addEventListener('scroll', handleScroll);
    }
    
    return () => {
      window.removeEventListener('resize', handleResize);
      if (containerRef.current) {
        containerRef.current.removeEventListener('scroll', handleScroll);
      }
    };
  }, []);

  // Generate path that routes around nodes when needed
  const generatePath = (connection) => {
    const { sourceX, sourceY, targetX, targetY, path } = connection;
    
    if (!path || path.type !== 'routed') {
      // Simple Bezier curve for direct connections
      const horizontalDistance = targetX - sourceX;
      const cp1X = sourceX + horizontalDistance * 0.5;
      const cp1Y = sourceY;
      const cp2X = sourceX + horizontalDistance * 0.5;
      const cp2Y = targetY;
      
      return `M ${sourceX} ${sourceY} C ${cp1X} ${cp1Y}, ${cp2X} ${cp2Y}, ${targetX} ${targetY}`;
    }
    
    // Routed path that curves above or below obstacles
    const { routingY } = path;
    const horizontalDistance = targetX - sourceX;
    
    // Create a smooth S-curve that goes through the routing level
    const midX = sourceX + horizontalDistance * 0.5;
    
    // Control points for smooth routing curve
    const cp1X = sourceX + horizontalDistance * 0.25;
    const cp1Y = sourceY + (routingY - sourceY) * 0.3;
    
    const cp2X = sourceX + horizontalDistance * 0.75;
    const cp2Y = targetY + (routingY - targetY) * 0.3;
    
    return `M ${sourceX} ${sourceY} C ${cp1X} ${cp1Y}, ${cp2X} ${cp2Y}, ${targetX} ${targetY}`;
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
            <div className={styles.layerNodes}>
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
        ))}
      </div>

      {/* SVG overlay for connections */}
      {showConnections && connections.length > 0 && (
        <svg 
          className={styles.connectionsOverlay}
          style={{
            width: '100%',
            height: '100%',
            position: 'absolute',
            top: 0,
            left: 0,
            pointerEvents: 'none'
          }}
        >
          <defs>
            {connections.map((connection, index) => (
              <marker
                key={`marker-${connection.id}`}
                id={`arrowhead-${connection.id}`}
                markerWidth="10"
                markerHeight="8"
                refX="0"
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
