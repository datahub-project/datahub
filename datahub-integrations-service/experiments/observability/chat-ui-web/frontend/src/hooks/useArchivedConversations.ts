import { useState, useCallback } from 'react';
import { apiClient } from '../api/client';
import type {
  ArchivedConversation,
  ConversationOriginType,
  ConversationSortBy,
} from '../api/types';

export function useArchivedConversations() {
  const [conversations, setConversations] = useState<ArchivedConversation[]>(
    []
  );
  const [currentConversation, setCurrentConversation] =
    useState<ArchivedConversation | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [total, setTotal] = useState(0);
  const [currentPage, setCurrentPage] = useState(0);
  const [originFilter, setOriginFilter] =
    useState<ConversationOriginType>(null);
  const [sortBy, setSortBy] = useState<ConversationSortBy>('max_thinking_time');
  const [sortDesc, setSortDesc] = useState<boolean>(true);

  const pageSize = 20;

  const loadConversations = useCallback(
    async (page: number = 0, originType?: ConversationOriginType, sortByParam?: ConversationSortBy, sortDescParam?: boolean) => {
      try {
        setLoading(true);
        setError(null);

        const result = await apiClient.listArchivedConversations(
          page * pageSize,
          pageSize,
          originType,
          sortByParam,
          sortDescParam
        );

        setConversations(result.conversations);
        setTotal(result.total);
        setCurrentPage(page);
        if (originType !== undefined) {
          setOriginFilter(originType);
        }
        if (sortByParam !== undefined) {
          setSortBy(sortByParam);
        }
        if (sortDescParam !== undefined) {
          setSortDesc(sortDescParam);
        }
      } catch (err) {
        setError(
          err instanceof Error
            ? err.message
            : 'Failed to load archived conversations'
        );
      } finally {
        setLoading(false);
      }
    },
    [pageSize]
  );

  const loadConversation = useCallback(async (urn: string) => {
    try {
      setLoading(true);
      setError(null);

      const conversation = await apiClient.getArchivedConversation(urn);
      setCurrentConversation(conversation);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : 'Failed to load conversation'
      );
    } finally {
      setLoading(false);
    }
  }, []);

  const nextPage = useCallback(() => {
    if ((currentPage + 1) * pageSize < total) {
      loadConversations(currentPage + 1, originFilter, sortBy, sortDesc);
    }
  }, [currentPage, total, pageSize, originFilter, sortBy, sortDesc, loadConversations]);

  const prevPage = useCallback(() => {
    if (currentPage > 0) {
      loadConversations(currentPage - 1, originFilter, sortBy, sortDesc);
    }
  }, [currentPage, originFilter, sortBy, sortDesc, loadConversations]);

  const filterByOrigin = useCallback(
    (originType: ConversationOriginType) => {
      setOriginFilter(originType);
      loadConversations(0, originType, sortBy, sortDesc);
    },
    [sortBy, sortDesc, loadConversations]
  );

  const changeSortBy = useCallback(
    (newSortBy: ConversationSortBy) => {
      setSortBy(newSortBy);
      loadConversations(0, originFilter, newSortBy, sortDesc);
    },
    [originFilter, sortDesc, loadConversations]
  );

  const toggleSortDirection = useCallback(() => {
    const newSortDesc = !sortDesc;
    setSortDesc(newSortDesc);
    loadConversations(0, originFilter, sortBy, newSortDesc);
  }, [sortDesc, originFilter, sortBy, loadConversations]);

  return {
    conversations,
    currentConversation,
    loading,
    error,
    total,
    currentPage,
    pageSize,
    originFilter,
    sortBy,
    sortDesc,
    loadConversations,
    loadConversation,
    nextPage,
    prevPage,
    filterByOrigin,
    changeSortBy,
    toggleSortDirection,
    hasNextPage: (currentPage + 1) * pageSize < total,
    hasPrevPage: currentPage > 0,
  };
}
