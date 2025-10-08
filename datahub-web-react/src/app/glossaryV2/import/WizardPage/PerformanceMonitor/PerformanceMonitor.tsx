import React, { useState } from 'react';
import styled from 'styled-components';
import { Card, Text, Heading, Button } from '@components';
import { Button as DataHubButton } from '@components';
import { PerformanceMetrics } from '../../shared/hooks/usePerformanceOptimization';


interface PerformanceMonitorProps {
  metrics: PerformanceMetrics;
  onOptimizeMemory: () => void;
  onClearCache: () => void;
  visible?: boolean;
}

const MonitorContainer = styled.div<{ visible: boolean }>`
  position: fixed;
  top: 20px;
  right: 20px;
  width: 300px;
  z-index: 1000;
  opacity: ${props => props.visible ? 1 : 0};
  visibility: ${props => props.visible ? 'visible' : 'hidden'};
  transition: opacity 0.3s ease, visibility 0.3s ease;
`;

const MonitorCard = styled(Card)`
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
`;

const MonitorHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
`;

const MonitorTitle = styled(Heading)`
  margin: 0 !important;
  font-size: 14px !important;
  font-weight: 600 !important;
  color: #111827 !important;
`;

const MetricItem = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
  
  &:last-child {
    margin-bottom: 0;
  }
`;

const MetricLabel = styled(Text)`
  font-size: 12px;
  color: #6b7280;
`;

const MetricValue = styled(Text)`
  font-size: 12px;
  font-weight: 500;
  color: #111827;
`;

const ProgressContainer = styled.div`
  margin: 8px 0;
`;

const ActionButtons = styled.div`
  display: flex;
  gap: 8px;
  margin-top: 12px;
`;

const ToggleButton = styled(DataHubButton)`
  position: fixed;
  top: 20px;
  right: 20px;
  z-index: 1001;
  border: none;
  background: rgba(0, 0, 0, 0.1);
  color: #6b7280;
  
  &:hover {
    background: rgba(0, 0, 0, 0.2);
    color: #374151;
  }
`;

const formatMemoryUsage = (bytes: number): string => {
  if (bytes === 0) return '0 B';
  
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

const formatTime = (ms: number): string => {
  if (ms < 1) return '< 1ms';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
};

const getPerformanceStatus = (renderTime: number): 'good' | 'warning' | 'poor' => {
  if (renderTime < 16) return 'good'; // 60fps
  if (renderTime < 100) return 'warning';
  return 'poor';
};

const getStatusColor = (status: 'good' | 'warning' | 'poor'): string => {
  switch (status) {
    case 'good':
      return '#10b981';
    case 'warning':
      return '#f59e0b';
    case 'poor':
      return '#ef4444';
  }
};

export const PerformanceMonitor: React.FC<PerformanceMonitorProps> = ({
  metrics,
  onOptimizeMemory,
  onClearCache,
  visible = false,
}) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const performanceStatus = getPerformanceStatus(metrics.renderTime);

  if (!visible) {
    return (
      <ToggleButton
        variant="text"
        icon={{ icon: 'ChartLine', source: 'phosphor' }}
        onClick={() => setIsExpanded(!isExpanded)}
      />
    );
  }

  return (
    <>
      <MonitorContainer visible={isExpanded}>
        <MonitorCard>
          <MonitorHeader>
            <MonitorTitle level={5}>
              Performance Monitor
            </MonitorTitle>
            <DataHubButton
              variant="text"
              icon={{ icon: 'X', source: 'phosphor' }}
              onClick={() => setIsExpanded(false)}
            />
          </MonitorHeader>

          <MetricItem>
            <MetricLabel>Render Time</MetricLabel>
            <MetricValue style={{ color: getStatusColor(performanceStatus) }}>
              {formatTime(metrics.renderTime)}
            </MetricValue>
          </MetricItem>

          <MetricItem>
            <MetricLabel>Memory Usage</MetricLabel>
            <MetricValue>
              {formatMemoryUsage(metrics.memoryUsage)}
            </MetricValue>
          </MetricItem>

          <MetricItem>
            <MetricLabel>Operations</MetricLabel>
            <MetricValue>
              {metrics.operationCount}
            </MetricValue>
          </MetricItem>

          <MetricItem>
            <MetricLabel>Last Operation</MetricLabel>
            <MetricValue>
              {metrics.lastOperation}
            </MetricValue>
          </MetricItem>

          <ProgressContainer>
            <Progress
              percent={Math.min((metrics.renderTime / 100) * 100, 100)}
              status={performanceStatus === 'poor' ? 'exception' : 'active'}
              strokeColor={getStatusColor(performanceStatus)}
              showInfo={false}
              size="small"
            />
          </ProgressContainer>

          <ActionButtons>
            <DataHubButton
              variant="outlined"
              size="small"
              onClick={onOptimizeMemory}
            >
              Optimize
            </DataHubButton>
            <DataHubButton
              variant="outlined"
              size="small"
              onClick={onClearCache}
            >
              Clear Cache
            </DataHubButton>
          </ActionButtons>
        </MonitorCard>
      </MonitorContainer>

      <ToggleButton
        variant="text"
        icon={{ icon: 'ChartLine', source: 'phosphor' }}
        onClick={() => setIsExpanded(!isExpanded)}
      />
    </>
  );
};
