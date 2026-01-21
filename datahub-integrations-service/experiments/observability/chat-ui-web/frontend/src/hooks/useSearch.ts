import { useState, useCallback } from 'react';
import { apiClient } from '../api/client';
import type { SearchResponse, ExplainResponse, RankingAnalysisResponse, SearchResultItem } from '../api/types';

const RESULTS_PER_PAGE = 20;

export function useSearch() {
  const [results, setResults] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [explainData, setExplainData] = useState<Record<string, ExplainResponse>>({});
  const [currentQuery, setCurrentQuery] = useState('');
  const [currentPage, setCurrentPage] = useState(0);

  // NEW: Selection and AI analysis state
  const [selectedUrns, setSelectedUrns] = useState<Set<string>>(new Set());
  const [selectedResults, setSelectedResults] = useState<Record<string, SearchResultItem>>({});
  const [aiAnalysis, setAiAnalysis] = useState<RankingAnalysisResponse | null>(null);
  const [analyzingRanking, setAnalyzingRanking] = useState(false);

  const search = useCallback(async (query: string, page: number = 0) => {
    if (!query.trim()) {
      setResults(null);
      setCurrentQuery('');
      setCurrentPage(0);
      return;
    }

    const isNewQuery = query.trim() !== currentQuery;

    setLoading(true);
    setError(null);
    setCurrentQuery(query.trim());
    setCurrentPage(page);

    // Only clear selections and explain data when searching a NEW query
    // Preserve selections when paginating through same query results
    if (isNewQuery) {
      setExplainData({});
      setSelectedUrns(new Set());
      setSelectedResults({});
      setAiAnalysis(null);
    }

    try {
      const response = await apiClient.search({
        query: query.trim(),
        start: page * RESULTS_PER_PAGE,
        count: RESULTS_PER_PAGE,
        types: [],
      });

      setResults(response);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Search failed';
      setError(errorMessage);
      setResults(null);
    } finally {
      setLoading(false);
    }
  }, [currentQuery]);

  const explainResult = useCallback(async (query: string, documentUrn: string, entityType: string) => {
    try {
      const response = await apiClient.explainScore({
        query,
        documentId: documentUrn,
        entityName: entityType.toLowerCase(),
      });

      setExplainData(prev => ({
        ...prev,
        [documentUrn]: response,
      }));

      return response;
    } catch (err) {
      console.error('Explain failed:', err);
      throw err;
    }
  }, []);

  // NEW: Toggle selection
  const toggleSelection = useCallback((urn: string, result: SearchResultItem) => {
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
  }, []);

  // NEW: Clear selections
  const clearSelections = useCallback(() => {
    setSelectedUrns(new Set());
    setSelectedResults({});
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
            const explainResponse = await explainResult(currentQuery, urn, result.entity.type);
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

          // Get position from the original search results when selected
          // Since we cache the result, we need to reconstruct position from current results or use a cached position
          const currentPageResult = results?.searchResults.find(r => r.entity.urn === urn);
          const position = currentPageResult
            ? results!.searchResults.indexOf(currentPageResult)
            : 0; // Fallback position if not on current page

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

      console.log(`Analyzing ${analysisItems.length} results:`, analysisItems.map(i => `${i.name} (pos ${i.position})`));

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
  }, [selectedUrns, selectedResults, currentQuery, explainData, explainResult, results]);

  const nextPage = useCallback(() => {
    if (currentQuery && results) {
      const totalPages = Math.ceil(results.total / RESULTS_PER_PAGE);
      if (currentPage < totalPages - 1) {
        search(currentQuery, currentPage + 1);
      }
    }
  }, [currentQuery, currentPage, results, search]);

  const previousPage = useCallback(() => {
    if (currentQuery && currentPage > 0) {
      search(currentQuery, currentPage - 1);
    }
  }, [currentQuery, currentPage, search]);

  const goToPage = useCallback((page: number) => {
    if (currentQuery) {
      search(currentQuery, page);
    }
  }, [currentQuery, search]);

  return {
    results,
    loading,
    error,
    explainData,
    currentPage,
    currentQuery,
    selectedUrns,
    aiAnalysis,
    analyzingRanking,
    search,
    explainResult,
    nextPage,
    previousPage,
    goToPage,
    toggleSelection,
    clearSelections,
    analyzeRanking,
  };
}
