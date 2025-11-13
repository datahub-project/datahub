import { colors } from '@components';
import { message as antMessage } from 'antd';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { ChatArea } from '@app/chat/components/ChatArea';
import { ConversationList } from '@app/chat/components/ConversationList';
import { ChatFeatureFlags } from '@app/chat/types';
import CompactContext from '@app/shared/CompactContext';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { useGetAuthenticatedUserUrn } from '@app/useGetAuthenticatedUser';
import { PageRoutes } from '@conf/Global';

import { useCreateDataHubAiConversationMutation, useListDataHubAiConversationsQuery } from '@graphql/aiChat.generated';
import { Entity } from '@types';

const LAST_CONVERSATION_KEY = 'datahub_last_conversation_urn';

const PageContainer = styled.div`
    display: flex;
    width: 100%;
    height: calc(100vh - 80px);
    gap: 8px;
`;

const MessengerContainer = styled.div`
    border-radius: 16px;
    border: 1px solid ${colors.gray[100]};
    display: flex;
    background-color: #ffffff;
    flex: 1;
    min-width: 0; /* Allow flex item to shrink */
    overflow: hidden; /* Prevent content from overflowing */
`;

const SidebarContainer = styled.div``;

const Sidebar = styled.div`
    width: 300px;
    border-right: 1px solid ${colors.gray[100]};
    display: flex;
    flex-direction: column;
`;

const MainContent = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-width: 0;
`;

const EntitySidebarContainer = styled.div<{ height: string }>`
    height: ${(props) => props.height};
    display: flex;
    flex-direction: column;
    position: sticky;
    top: 0;
    border-radius: 10px;
    overflow: hidden;
`;

/**
 * Main Chat Page component
 * Shows a sidebar with conversation list and main chat area
 */
export const ChatPage = () => {
    const history = useHistory();
    const location = useLocation<{ initialMessage?: string }>();
    const userUrn = useGetAuthenticatedUserUrn();
    const entityRegistry = useEntityRegistryV2();
    const sidebarWidth = useSidebarWidth();
    const pageContainerRef = useRef<HTMLDivElement>(null);

    // Extract conversation URN from URL query params
    const searchParams = new URLSearchParams(location.search);
    const selectedConversationUrn = searchParams.get('conversation');

    // Extract initial message from location state (from SearchBar "Ask DataHub")
    // Store it in a ref so it persists across navigation
    const initialMessageRef = useRef<string | undefined>(location.state?.initialMessage);

    // Update ref if location state changes (only on first mount)
    useEffect(() => {
        if (location.state?.initialMessage && !initialMessageRef.current) {
            initialMessageRef.current = location.state.initialMessage;
        }
    }, [location.state]);

    // Clear the initial message from location state to prevent re-sending on refresh
    useEffect(() => {
        if (location.state?.initialMessage) {
            history.replace(location.pathname + location.search);
        }
    }, [location.state, history, location]);

    const [featureFlags] = useState<ChatFeatureFlags>({
        verboseMode: false,
    });

    const [hasAutoCreated, setHasAutoCreated] = useState(false);
    const [optimisticConversation, setOptimisticConversation] = useState<any>(null);
    const [selectedEntity, setSelectedEntity] = useState<Entity | null>(null);
    const [isSidebarClosed, setIsSidebarClosed] = useState(false);

    // Fetch conversations list - TODO: Add pagination / infinite scroll
    const {
        data: conversationsData,
        loading: loadingConversations,
        refetch: refetchConversations,
    } = useListDataHubAiConversationsQuery({
        variables: { count: 50, start: 0 },
        fetchPolicy: 'cache-and-network',
    });

    const [createConversation, { loading: creatingConversation }] = useCreateDataHubAiConversationMutation();

    const conversations = useMemo(() => {
        const baseConversations = conversationsData?.listDataHubAiConversations?.conversations || [];

        // Sort conversations by most recent activity to show active conversations at the top.
        // This provides better UX as users typically want to continue recent conversations.
        // Note: Relies on backend updating lastUpdated timestamp when messages are sent.
        // Fallback to created.time ensures new conversations without messages sort correctly.
        const sortedConversations = [...baseConversations].sort((a, b) => {
            const aTime = a.lastUpdated?.time || a.created?.time || 0;
            const bTime = b.lastUpdated?.time || b.created?.time || 0;
            return bTime - aTime;
        });

        // If we have an optimistic conversation and it's not in the list yet, add it
        if (optimisticConversation && !sortedConversations.some((c) => c.urn === optimisticConversation.urn)) {
            return [optimisticConversation, ...sortedConversations];
        }

        return sortedConversations;
    }, [conversationsData, optimisticConversation]);

    // Handle creating a new conversation
    const handleCreateConversation = useCallback(
        async (silent = false) => {
            try {
                const result = await createConversation({
                    variables: {
                        input: {
                            title: null, // Title will be set from first message
                        },
                    },
                });

                if (result.data?.createDataHubAiConversation) {
                    const newConversation = result.data.createDataHubAiConversation;

                    // Add optimistic conversation immediately
                    setOptimisticConversation(newConversation);

                    // Emit analytics event for chat creation
                    // Origin is 'search_bar' if there's an initialMessage, otherwise 'manual'
                    analytics.event({
                        type: EventType.CreateDataHubChatEvent,
                        origin: initialMessageRef.current ? 'search_bar' : 'manual',
                        conversationUrn: newConversation.urn,
                    });

                    // Navigate to the new conversation
                    history.push(`${PageRoutes.AI_CHAT}?conversation=${newConversation.urn}`);

                    // Refetch in background to ensure consistency (wait for eventual consistency)
                    setTimeout(() => {
                        refetchConversations();
                        setOptimisticConversation(null); // Clear optimistic state after refetch
                    }, 10000);
                }
            } catch (error) {
                console.error('Failed to create conversation:', error);
                if (!silent) {
                    antMessage.error('Failed to create new conversation');
                }
            }
        },
        [createConversation, history, refetchConversations],
    );

    // Auto-create or select conversation on mount
    useEffect(() => {
        if (loadingConversations || hasAutoCreated) {
            return;
        }

        // If no conversation is selected in URL
        if (!selectedConversationUrn) {
            // If we have an initialMessage (from "Ask DataHub"), always create a new conversation
            if (initialMessageRef.current) {
                setHasAutoCreated(true);
                handleCreateConversation(true);
                return;
            }

            // Try to get last conversation from local storage
            const lastConversationUrn = localStorage.getItem(LAST_CONVERSATION_KEY);

            // If we have a last conversation and it exists in the list, select it
            if (lastConversationUrn && conversations.some((c) => c.urn === lastConversationUrn)) {
                history.replace(`${PageRoutes.AI_CHAT}?conversation=${lastConversationUrn}`);
                return;
            }

            // Otherwise, auto-create a new conversation
            setHasAutoCreated(true);
            handleCreateConversation(true);
        }
    }, [
        selectedConversationUrn,
        conversations,
        loadingConversations,
        hasAutoCreated,
        history,
        handleCreateConversation,
    ]);

    // Save selected conversation to local storage
    useEffect(() => {
        if (selectedConversationUrn) {
            localStorage.setItem(LAST_CONVERSATION_KEY, selectedConversationUrn);
        }
    }, [selectedConversationUrn]);

    // Handle selecting a conversation
    const handleSelectConversation = (conversationUrn: string) => {
        history.push(`${PageRoutes.AI_CHAT}?conversation=${conversationUrn}`);
    };

    // Handle deleting a conversation
    const handleDeleteConversation = async () => {
        // Refetch conversations after deletion
        await refetchConversations();

        // If the deleted conversation was selected, navigate away
        if (selectedConversationUrn) {
            history.push(PageRoutes.AI_CHAT);
        }
    };

    // Handle entity selection from references
    const handleEntitySelect = useCallback(
        (entity: Entity | null) => {
            if (!entity) {
                setSelectedEntity(null);
                setIsSidebarClosed(true);
            } else if (!selectedEntity || entity.urn !== selectedEntity.urn) {
                setIsSidebarClosed(false);
                setSelectedEntity(entity);
            } else if (selectedEntity?.urn === entity.urn) {
                setIsSidebarClosed(true);
                setSelectedEntity(null);
            }
        },
        [selectedEntity],
    );

    // Handle clicking outside to close sidebar
    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const target = event.target as HTMLElement;

            // Check if click is outside the reference cards and entity sidebar
            const isClickInSidebar = target.closest('[data-testid="entity-sidebar"]');
            const isClickInReferenceCard = target.closest('[data-reference-card]');

            if (!isClickInSidebar && !isClickInReferenceCard && selectedEntity) {
                setSelectedEntity(null);
                setIsSidebarClosed(true);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, [selectedEntity]);

    return (
        <PageContainer ref={pageContainerRef}>
            <MessengerContainer>
                <Sidebar>
                    <ConversationList
                        conversations={conversations}
                        selectedConversationUrn={selectedConversationUrn || undefined}
                        onSelectConversation={handleSelectConversation}
                        onCreateConversation={handleCreateConversation}
                        onDeleteConversation={handleDeleteConversation}
                        loading={loadingConversations}
                        creatingConversation={creatingConversation}
                    />
                </Sidebar>
                <MainContent>
                    {selectedConversationUrn && (
                        <ChatArea
                            conversationUrn={selectedConversationUrn}
                            userUrn={userUrn}
                            featureFlags={featureFlags}
                            onConversationUpdate={refetchConversations}
                            selectedEntityUrn={selectedEntity?.urn}
                            onEntitySelect={handleEntitySelect}
                            initialMessage={initialMessageRef.current}
                        />
                    )}
                </MainContent>
            </MessengerContainer>
            {selectedEntity && !isSidebarClosed && (
                <SidebarContainer>
                    <EntitySidebarContext.Provider
                        value={{ width: sidebarWidth, isClosed: isSidebarClosed, setSidebarClosed: setIsSidebarClosed }}
                    >
                        <EntitySidebarContainer key={selectedEntity.urn} data-testid="entity-sidebar" height="100%">
                            <CompactContext.Provider value>
                                {entityRegistry.renderProfile(selectedEntity.type, selectedEntity.urn)}
                            </CompactContext.Provider>
                        </EntitySidebarContainer>
                    </EntitySidebarContext.Provider>
                </SidebarContainer>
            )}
        </PageContainer>
    );
};
