import React, { useCallback } from 'react';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  addEdge,
} from 'reactflow';
import 'reactflow/dist/style.css';
import styles from './styles.module.css';

const Discovery Challenge #2: The Data Detective = ({ 
  nodes: initialNodes = [], 
  edges: initialEdges = [],
  title,
  height = '400px',
  showMiniMap = true,
  showControls = true,
  showBackground = true,
  backgroundType = 'dots'
}) => {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  const onConnect = useCallback(
    (params) => setEdges((eds) => addEdge(params, eds)),
    [setEdges],
  );

  return (
    <div className={styles.diagramContainer}>
      {title && <h4 className={styles.diagramTitle}>{title}</h4>}
      <div style={{ width: '100%', height }} className={styles.reactFlowWrapper}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          className={styles.reactFlow}
          fitView
          attributionPosition="bottom-left"
        >
          {showControls && <Controls className={styles.controls} />}
          {showMiniMap && (
            <MiniMap 
              className={styles.miniMap}
              nodeColor="#1890ff"
              nodeStrokeWidth={2}
              zoomable
              pannable
            />
          )}
          {showBackground && (
            <Background 
              variant={backgroundType}
              gap={12}
              size={1}
              className={styles.background}
            />
          )}
        </ReactFlow>
      </div>
    </div>
  );
};

// Pre-defined diagram configurations for common DataHub workflows
export const DataHubWorkflows = {
  ingestionFlow: {
    nodes: [
      {
        id: '1',
        type: 'input',
        data: { label: '🗄️ Data Sources\n(Kafka, Hive, HDFS)' },
        position: { x: 0, y: 0 },
        className: 'source-node',
      },
      {
        id: '2',
        data: { label: '⚙️ DataHub Ingestion\nExtract Metadata' },
        position: { x: 200, y: 0 },
        className: 'process-node',
      },
      {
        id: '3',
        data: { label: '📊 Metadata Storage\nElasticsearch + MySQL' },
        position: { x: 400, y: 0 },
        className: 'storage-node',
      },
      {
        id: '4',
        type: 'output',
        data: { label: '🔍 DataHub UI\nDiscovery & Lineage' },
        position: { x: 600, y: 0 },
        className: 'output-node',
      },
    ],
    edges: [
      { id: 'e1-2', source: '1', target: '2', animated: true, label: 'metadata' },
      { id: 'e2-3', source: '2', target: '3', animated: true, label: 'store' },
      { id: 'e3-4', source: '3', target: '4', animated: true, label: 'serve' },
    ],
  },
  
  discoveryFlow: {
    nodes: [
      {
        id: '1',
        type: 'input',
        data: { label: '👤 Data Analyst\nNeeds user metrics' },
        position: { x: 0, y: 100 },
        className: 'user-node',
      },
      {
        id: '2',
        data: { label: '🔍 Search DataHub\n"user created deleted"' },
        position: { x: 200, y: 0 },
        className: 'search-node',
      },
      {
        id: '3',
        data: { label: '📋 Browse Results\nfct_users_created' },
        position: { x: 200, y: 100 },
        className: 'browse-node',
      },
      {
        id: '4',
        data: { label: '📊 Examine Schema\nColumns & Types' },
        position: { x: 200, y: 200 },
        className: 'schema-node',
      },
      {
        id: '5',
        type: 'output',
        data: { label: '✅ Found Data\nReady for Analysis' },
        position: { x: 400, y: 100 },
        className: 'success-node',
      },
    ],
    edges: [
      { id: 'e1-2', source: '1', target: '2', label: 'search' },
      { id: 'e1-3', source: '1', target: '3', label: 'browse' },
      { id: 'e1-4', source: '1', target: '4', label: 'explore' },
      { id: 'e2-5', source: '2', target: '5' },
      { id: 'e3-5', source: '3', target: '5' },
      { id: 'e4-5', source: '4', target: '5' },
    ],
  },

  lineageFlow: {
    nodes: [
      {
        id: '1',
        data: { label: '📥 Raw Events\nKafka Stream' },
        position: { x: 0, y: 0 },
        className: 'source-node',
      },
      {
        id: '2',
        data: { label: '⚙️ ETL Process\nSpark Job' },
        position: { x: 200, y: 0 },
        className: 'process-node',
      },
      {
        id: '3',
        data: { label: '🗄️ Analytics Table\nfct_users_created' },
        position: { x: 400, y: 0 },
        className: 'table-node',
      },
      {
        id: '4',
        data: { label: '📊 Dashboard\nUser Metrics' },
        position: { x: 600, y: 0 },
        className: 'output-node',
      },
      {
        id: '5',
        data: { label: '🔧 Data Quality\nValidation Rules' },
        position: { x: 200, y: 100 },
        className: 'quality-node',
      },
    ],
    edges: [
      { id: 'e1-2', source: '1', target: '2', animated: true, label: 'raw data' },
      { id: 'e2-3', source: '2', target: '3', animated: true, label: 'processed' },
      { id: 'e3-4', source: '3', target: '4', animated: true, label: 'visualize' },
      { id: 'e2-5', source: '2', target: '5', label: 'validate' },
      { id: 'e5-3', source: '5', target: '3', label: 'quality check' },
    ],
  },
};

export default InteractiveDiagram;
