import { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import { Entity } from '../../glossary.types';

export interface PerformanceMetrics {
  renderTime: number;
  memoryUsage: number;
  operationCount: number;
  lastOperation: string;
}

export interface UsePerformanceOptimizationProps {
  entities: Entity[];
  pageSize: number;
  enableVirtualScrolling?: boolean;
  enableDebouncing?: boolean;
  debounceDelay?: number;
}

export interface UsePerformanceOptimizationReturn {
  metrics: PerformanceMetrics;
  optimizedEntities: Entity[];
  debouncedSearch: (query: string) => void;
  virtualizedData: Entity[];
  clearCache: () => void;
  optimizeMemory: () => void;
}

export const usePerformanceOptimization = ({
  entities,
  pageSize,
  enableVirtualScrolling = true,
  enableDebouncing = true,
  debounceDelay = 300,
}: UsePerformanceOptimizationProps): UsePerformanceOptimizationReturn => {
  const [metrics, setMetrics] = useState<PerformanceMetrics>({
    renderTime: 0,
    memoryUsage: 0,
    operationCount: 0,
    lastOperation: '',
  });

  const [searchQuery, setSearchQuery] = useState('');
  const [debouncedQuery, setDebouncedQuery] = useState('');
  const cacheRef = useRef<Map<string, Entity[]>>(new Map());
  const renderStartRef = useRef<number>(0);

  // Performance monitoring
  const updateMetrics = useCallback((operation: string) => {
    const renderTime = performance.now() - renderStartRef.current;
    const memoryUsage = (performance as any).memory?.usedJSHeapSize || 0;
    
    setMetrics(prev => ({
      renderTime,
      memoryUsage,
      operationCount: prev.operationCount + 1,
      lastOperation: operation,
    }));
  }, []);

  // Debounced search
  const debouncedSearch = useCallback((query: string) => {
    setSearchQuery(query);
    
    if (enableDebouncing) {
      const timeoutId = setTimeout(() => {
        setDebouncedQuery(query);
      }, debounceDelay);
      
      return () => clearTimeout(timeoutId);
    } else {
      setDebouncedQuery(query);
    }
  }, [enableDebouncing, debounceDelay]);

  // Memoized filtering
  const filteredEntities = useMemo(() => {
    renderStartRef.current = performance.now();
    
    if (!debouncedQuery.trim()) {
      updateMetrics('filter_none');
      return entities;
    }

    const cacheKey = `filter_${debouncedQuery}`;
    if (cacheRef.current.has(cacheKey)) {
      updateMetrics('filter_cache_hit');
      return cacheRef.current.get(cacheKey)!;
    }

    const filtered = entities.filter(entity =>
      entity.name.toLowerCase().includes(debouncedQuery.toLowerCase()) ||
      entity.data.description?.toLowerCase().includes(debouncedQuery.toLowerCase()) ||
      entity.data.parent_nodes?.toLowerCase().includes(debouncedQuery.toLowerCase())
    );

    cacheRef.current.set(cacheKey, filtered);
    updateMetrics('filter_compute');
    return filtered;
  }, [entities, debouncedQuery, updateMetrics]);

  // Virtual scrolling optimization
  const virtualizedData = useMemo(() => {
    if (!enableVirtualScrolling || filteredEntities.length <= pageSize * 2) {
      return filteredEntities;
    }

    // For now, return all data - virtual scrolling would be implemented
    // with a library like react-window or react-virtualized
    return filteredEntities;
  }, [filteredEntities, enableVirtualScrolling, pageSize]);

  // Optimized entities with memoization
  const optimizedEntities = useMemo(() => {
    renderStartRef.current = performance.now();
    
    const optimized = virtualizedData.map(entity => ({
      ...entity,
      // Pre-compute expensive operations
      displayName: entity.name,
      statusColor: getStatusColor(entity.status),
      hasParent: entity.parentNames.length > 0,
    }));

    updateMetrics('optimize_entities');
    return optimized;
  }, [virtualizedData, updateMetrics]);

  // Memory optimization
  const optimizeMemory = useCallback(() => {
    // Clear old cache entries
    if (cacheRef.current.size > 100) {
      const entries = Array.from(cacheRef.current.entries());
      const toKeep = entries.slice(-50); // Keep last 50 entries
      cacheRef.current.clear();
      toKeep.forEach(([key, value]) => {
        cacheRef.current.set(key, value);
      });
    }

    // Force garbage collection if available
    if ((window as any).gc) {
      (window as any).gc();
    }

    updateMetrics('memory_optimization');
  }, [updateMetrics]);

  // Clear cache
  const clearCache = useCallback(() => {
    cacheRef.current.clear();
    updateMetrics('cache_clear');
  }, [updateMetrics]);

  // Monitor memory usage
  useEffect(() => {
    const interval = setInterval(() => {
      if ((performance as any).memory) {
        const memoryUsage = (performance as any).memory.usedJSHeapSize;
        setMetrics(prev => ({
          ...prev,
          memoryUsage,
        }));
      }
    }, 5000);

    return () => clearInterval(interval);
  }, []);

  return {
    metrics,
    optimizedEntities,
    debouncedSearch,
    virtualizedData,
    clearCache,
    optimizeMemory,
  };
};

// Helper function
function getStatusColor(status: string): string {
  switch (status) {
    case 'new':
      return '#10b981';
    case 'updated':
      return '#3b82f6';
    case 'conflict':
      return '#ef4444';
    default:
      return '#6b7280';
  }
}
