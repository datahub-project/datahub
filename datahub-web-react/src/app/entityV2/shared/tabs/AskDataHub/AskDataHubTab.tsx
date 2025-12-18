import React, { useEffect } from 'react';
import styled from 'styled-components';

import { ChatArea } from '@app/chat/components/ChatArea';
import { ChatMessageAction, ChatVariant } from '@app/chat/types';
import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { useAskDataHubContext } from '@app/entityV2/shared/tabs/AskDataHub/AskDataHubContext';
import { useEntityConversation } from '@app/entityV2/shared/tabs/AskDataHub/hooks/useEntityConversation';
import { EntityTabProps } from '@app/entityV2/shared/types';
import { useEntityRegistry } from '@src/app/useEntityRegistry';

const ChatContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: hidden;
`;

const SUGGESTED_QUESTIONS = ['What is this used for?', 'Who are the main users?'];

/**
 * Contextual AI chat integrated into entity sidebar to provide immediate assistance
 * without navigating away. Persists conversation state to enable seamless transitions
 * between sidebar and full chat page, improving user flow when exploring entities.
 */
export default function AskDataHubTab(_props: EntityTabProps) {
    const entityContext = useEntityData();
    const entityRegistry = useEntityRegistry();
    const userContext = useUserContext();
    const { conversationUrn: persistedConversationUrn, setConversationUrn } = useAskDataHubContext();

    const { entityData, urn: entityUrn = '', entityType } = entityContext || {};
    const userUrn = userContext.user?.urn;
    const featureFlags = { verboseMode: false };
    const compactActions: ChatMessageAction[] = [ChatMessageAction.Copy, ChatMessageAction.OpenInChat];

    // Provide generic fallback so user can still chat even if entity data is loading
    const entityName = entityData ? entityRegistry.getDisplayName(entityType, entityData) : 'this asset';

    // Encapsulates complex conversation lifecycle (creation, entity context, state management)
    // to keep this component focused on UI orchestration rather than chat logic
    const {
        conversationUrn: newConversationUrn,
        initialMessage,
        startConversation,
    } = useEntityConversation({
        entityUrn,
        entityName,
        userUrn,
    });

    // Use persisted conversation if available, otherwise use new one
    const conversationUrn = persistedConversationUrn || newConversationUrn;

    // Persist new conversation to context so it survives tab switches
    useEffect(() => {
        if (newConversationUrn) {
            setConversationUrn(newConversationUrn);
        }
    }, [newConversationUrn, setConversationUrn]);

    // Prevent runtime errors by ensuring entity data is loaded before accessing entity properties
    // (e.g., entityType, entityData) needed for conversation initialization
    if (!entityContext || !userUrn) {
        return null;
    }

    // ChatArea now handles both the welcome state (no conversation) and active chat state
    return (
        <ChatContainer>
            <ChatArea
                conversationUrn={conversationUrn ?? undefined}
                userUrn={userUrn}
                featureFlags={featureFlags}
                initialMessage={initialMessage ?? undefined}
                variant={ChatVariant.Compact}
                messageActions={compactActions}
                showReferences={false}
                suggestedQuestions={SUGGESTED_QUESTIONS}
                onStartConversation={startConversation}
                welcomePlaceholder={`Ask about ${entityName}...`}
            />
        </ChatContainer>
    );
}
