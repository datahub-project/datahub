import { useState, useCallback, useRef, useEffect } from 'react';
import { apiClient } from '../api/client';
import type { SearchResponse, ExplainResponse, RankingAnalysisResponse, SearchResultItem } from '../api/types';

const DEFAULT_RESULTS_PER_PAGE = 20;

export function useSearch() {
  const [results, setResults] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [explainData, setExplainData] = useState<Record<string, ExplainResponse>>({});
  const [currentQuery, setCurrentQuery] = useState('');
  const [currentTypes, setCurrentTypes] = useState<string[] | undefined>(undefined);
  const [currentPage, setCurrentPage] = useState(0);
  const [resultsPerPage, setResultsPerPage] = useState(DEFAULT_RESULTS_PER_PAGE);

  // Search type and latency tracking
  const [searchType, setSearchType] = useState<'QUERY_THEN_FETCH' | 'DFS_QUERY_THEN_FETCH'>('DFS_QUERY_THEN_FETCH');
  const [latency, setLatency] = useState<number | null>(null);
  const currentQueryRef = useRef(currentQuery);
  const currentTypesRef = useRef(currentTypes);

  // Store current search overrides so pagination can reuse them
  const currentOverrides = useRef<{
    functionScoreOverride?: string;
    rescoreEnabled?: boolean;
    rescoreFormulaOverride?: string;
    rescoreSignalsOverride?: string;
  }>({});

  // Selection and AI analysis state
  const [selectedUrns, setSelectedUrns] = useState<Set<string>>(new Set());
  const [selectedResults, setSelectedResults] = useState<Record<string, SearchResultItem>>({});
  // Track absolute position (1-based) at time of selection for cross-page accuracy
  const [selectedPositions, setSelectedPositions] = useState<Record<string, number>>({});
  const [aiAnalysis, setAiAnalysis] = useState<RankingAnalysisResponse | null>(null);
  const [analyzingRanking, setAnalyzingRanking] = useState(false);

  useEffect(() => {
    currentQueryRef.current = currentQuery;
    currentTypesRef.current = currentTypes;
  }, [currentQuery, currentTypes]);

  const search = useCallback(async (
    query: string,
    types?: string[],
    page: number = 0,
    searchTypeOverride?: 'QUERY_THEN_FETCH' | 'DFS_QUERY_THEN_FETCH',
    pageSizeOverride?: number,
    functionScoreOverride?: string,
    rescoreEnabled?: boolean,
    rescoreFormulaOverride?: string,
    rescoreSignalsOverride?: string
  ) => {
    if (!query.trim()) {
      setResults(null);
      setCurrentQuery('');
      setCurrentTypes(undefined);
      setCurrentPage(0);
      setLatency(null);
      return;
    }

    const previousOverrides = currentOverrides.current;
    const overridesChanged =
      functionScoreOverride !== previousOverrides.functionScoreOverride ||
      rescoreEnabled !== previousOverrides.rescoreEnabled ||
      rescoreFormulaOverride !== previousOverrides.rescoreFormulaOverride ||
      rescoreSignalsOverride !== previousOverrides.rescoreSignalsOverride;

    const isNewQuery =
      query.trim() !== currentQueryRef.current ||
      JSON.stringify(types) !== JSON.stringify(currentTypesRef.current);
    const shouldResetSelectionState = isNewQuery || overridesChanged;

    // Persist overrides so pagination can reuse them
    currentOverrides.current = {
      functionScoreOverride, rescoreEnabled,
      rescoreFormulaOverride, rescoreSignalsOverride,
    };

    setLoading(true);
    setError(null);
    setCurrentQuery(query.trim());
    setCurrentTypes(types);
    setCurrentPage(page);
    currentQueryRef.current = query.trim();
    currentTypesRef.current = types;

    // Clear selection/explain state when the search context changes.
    // Preserve selections when only paginating through the same query + overrides.
    if (shouldResetSelectionState) {
      setExplainData({});
      setSelectedUrns(new Set());
      setSelectedResults({});
      setSelectedPositions({});
      setAiAnalysis(null);
    }

    // Prepare request parameters before timing
    const pageSize = pageSizeOverride !== undefined ? pageSizeOverride : resultsPerPage;
    const searchQuery = query.trim();
    const searchStart = page * pageSize;
    const searchTypes = types || [];
    const effectiveSearchType = searchTypeOverride || searchType;

    try {
      // Start timing - measure ONLY the API call (network + backend processing)
      const startTime = performance.now();

      const response = await apiClient.search({
        query: searchQuery,
        start: searchStart,
        count: pageSize,
        types: searchTypes,
        searchType: effectiveSearchType,
        functionScoreOverride: functionScoreOverride,
        rescoreEnabled: rescoreEnabled,
        rescoreFormulaOverride: rescoreFormulaOverride,
        rescoreSignalsOverride: rescoreSignalsOverride,
      });

      // Stop timing immediately after API response (before any UI state updates)
      const endTime = performance.now();
      const searchLatency = Math.round(endTime - startTime);

      // Now update UI state (not included in latency measurement)
      setLatency(searchLatency);
      setResults(response);

      // Extract embedded explanations into cache using functional update
      // to avoid stale closure over explainData
      const newItems: Record<string, ExplainResponse> = {};
      for (const result of response.searchResults) {
        if (result.explanation) {
          newItems[result.entity.urn] = {
            index: result.entity.type.toLowerCase() + 'index',
            documentId: result.entity.urn,
            matched: result.explanation.match ?? true,
            score: result.score,
            singleIndexScore: result.score,
            searchedEntityTypes: [],
            explanation: result.explanation,
          };
        }
      }

      if (Object.keys(newItems).length > 0) {
        setExplainData(prev => ({ ...prev, ...newItems }));
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Search failed';
      setError(errorMessage);
      setResults(null);
    } finally {
      setLoading(false);
    }
  }, [currentQuery, currentTypes, searchType, resultsPerPage]);

  const explainResult = useCallback(async (documentUrn: string) => {
    // Check if already in cache (from embedded explain data)
    if (explainData[documentUrn]) {
      return explainData[documentUrn];
    }

    // No fallback to separate API - explanation data comes from embedded _explain in search results
    console.warn(`Explanation not available for ${documentUrn}. Result may not have been in the search response.`);
    return null;
  }, [explainData]);

  // Toggle selection with absolute position tracking
  const toggleSelection = useCallback((urn: string, result: SearchResultItem, position: number) => {
    setSelectedUrns(prev => {
      const newSet = new Set(prev);
      if (newSet.has(urn)) {
        newSet.delete(urn);
      } else {
        newSet.add(urn);
      }
      return newSet;
    });

    setSelectedResults(prev => {
      const newResults = { ...prev };
      if (newResults[urn]) {
        delete newResults[urn];
      } else {
        newResults[urn] = result;
      }
      return newResults;
    });

    setSelectedPositions(prev => {
      const newPositions = { ...prev };
      if (newPositions[urn]) {
        delete newPositions[urn];
      } else {
        newPositions[urn] = position;
      }
      return newPositions;
    });
  }, []);

  const clearSelections = useCallback(() => {
    setSelectedUrns(new Set());
    setSelectedResults({});
    setSelectedPositions({});
  }, []);

  // NEW: Analyze ranking
  const analyzeRanking = useCallback(async () => {
    if (selectedUrns.size === 0 || !currentQuery) {
      return;
    }

    setAnalyzingRanking(true);
    setError(null);

    try {
      // Fetch explain data for all selected results that don't have it yet
      const missingExplains = Array.from(selectedUrns).filter(urn => !explainData[urn]);

      // Collect the newly fetched explain data
      const newExplainData: Record<string, ExplainResponse> = { ...explainData };

      await Promise.all(
        missingExplains.map(async (urn) => {
          const result = selectedResults[urn];
          if (result) {
            const explainResponse = await explainResult(urn);
            if (explainResponse) {
              newExplainData[urn] = explainResponse;
            }
          }
        })
      );

      // Build analysis request using cached selected results
      const analysisItems = Array.from(selectedUrns)
        .map(urn => {
          const result = selectedResults[urn];
          const explain = newExplainData[urn];
          if (!result || !explain) {
            console.warn(`Missing data for URN ${urn}: result=${!!result}, explain=${!!explain}`);
            return null;
          }

          // Use the absolute position captured at selection time
          const position = selectedPositions[urn] ?? 0;

          return {
            urn: result.entity.urn,
            type: result.entity.type,
            name: result.entity.name || result.entity.urn,
            position,
            score: result.score,
            matched: explain.matched,
            explanation: explain.explanation,
          };
        })
        .filter((item): item is NonNullable<typeof item> => item !== null)
        .sort((a, b) => a.position - b.position); // Sort by position

      if (analysisItems.length === 0) {
        throw new Error('No valid results to analyze. Please ensure explain data is available for selected items.');
      }

      // Call AI analysis endpoint
      const response = await apiClient.analyzeRanking({
        query: currentQuery,
        results: analysisItems,
      });

      setAiAnalysis(response);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Analysis failed';
      setError(errorMessage);
      console.error('Ranking analysis failed:', err);
    } finally {
      setAnalyzingRanking(false);
    }
  }, [selectedUrns, selectedResults, selectedPositions, currentQuery, explainData, explainResult]);

  const nextPage = useCallback(() => {
    if (currentQuery && results) {
      const totalPages = Math.ceil(results.total / resultsPerPage);
      if (currentPage < totalPages - 1) {
        const o = currentOverrides.current;
        search(currentQuery, currentTypes, currentPage + 1, undefined, undefined,
          o.functionScoreOverride, o.rescoreEnabled,
          o.rescoreFormulaOverride, o.rescoreSignalsOverride);
      }
    }
  }, [currentQuery, currentTypes, currentPage, results, resultsPerPage, search]);

  const previousPage = useCallback(() => {
    if (currentQuery && currentPage > 0) {
      const o = currentOverrides.current;
      search(currentQuery, currentTypes, currentPage - 1, undefined, undefined,
        o.functionScoreOverride, o.rescoreEnabled,
        o.rescoreFormulaOverride, o.rescoreSignalsOverride);
    }
  }, [currentQuery, currentTypes, currentPage, search]);

  const goToPage = useCallback((page: number) => {
    if (currentQuery) {
      const o = currentOverrides.current;
      search(currentQuery, currentTypes, page, undefined, undefined,
        o.functionScoreOverride, o.rescoreEnabled,
        o.rescoreFormulaOverride, o.rescoreSignalsOverride);
    }
  }, [currentQuery, currentTypes, search]);

  const changeResultsPerPage = useCallback((newSize: number) => {
    setResultsPerPage(newSize);
    if (currentQuery) {
      const o = currentOverrides.current;
      search(currentQuery, currentTypes, 0, undefined, newSize,
        o.functionScoreOverride, o.rescoreEnabled,
        o.rescoreFormulaOverride, o.rescoreSignalsOverride);
    }
  }, [currentQuery, currentTypes, search]);

  return {
    results,
    loading,
    error,
    explainData,
    currentPage,
    currentQuery,
    searchType,
    setSearchType,
    latency,
    resultsPerPage,
    selectedUrns,
    aiAnalysis,
    analyzingRanking,
    search,
    explainResult,
    nextPage,
    previousPage,
    goToPage,
    changeResultsPerPage,
    toggleSelection,
    clearSelections,
    analyzeRanking,
  };
}
