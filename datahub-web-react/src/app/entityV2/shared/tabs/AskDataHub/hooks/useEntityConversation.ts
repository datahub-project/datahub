import { useCallback, useState } from 'react';
import { useHistory } from 'react-router-dom';

import { PageRoutes } from '@conf/Global';

import { useCreateDataHubAiConversationMutation } from '@graphql/aiChat.generated';

interface UseEntityConversationProps {
    entityUrn: string;
    entityName: string;
    userUrn?: string;
}

interface UseEntityConversationReturn {
    conversationUrn: string | null;
    initialMessage: string | null;
    isCreating: boolean;
    /** Returns true if conversation was started successfully, false if missing required context */
    startConversation: (question: string) => Promise<boolean>;
}

/**
 * Hook to manage conversation creation for entity-specific chats
 * Handles conversation creation and fallback to main chat page
 */
export const useEntityConversation = ({
    entityUrn,
    entityName,
    userUrn,
}: UseEntityConversationProps): UseEntityConversationReturn => {
    const [conversationUrn, setConversationUrn] = useState<string | null>(null);
    const [initialMessage, setInitialMessage] = useState<string | null>(null);
    const [createConversation, { loading: isCreating }] = useCreateDataHubAiConversationMutation();
    const history = useHistory();

    const startConversation = useCallback(
        async (question: string): Promise<boolean> => {
            if (!entityUrn || !userUrn) {
                // Missing required context - cannot create conversation without entity or user URN
                // This typically indicates entity context not initialized or user session issue
                return false;
            }

            // Embed entity context so AI can reference the specific asset in its response
            // URL-encode URN because markdown link syntax [text](url) breaks when URLs contain
            // unescaped parentheses - common in column URNs like urn:li:schemaField:(dataset,field)
            // Escape brackets in entity name to prevent breaking markdown link syntax (e.g., dataset[v1])
            const encodedUrn = encodeURIComponent(entityUrn);
            const escapedEntityName = entityName.replace(/\[/g, '\\[').replace(/\]/g, '\\]');
            const messageWithEntity = `[@${escapedEntityName}](${encodedUrn}) ${question}`;

            try {
                const result = await createConversation({
                    variables: {
                        input: {
                            title: null, // Title will be set from first message
                        },
                    },
                });

                const newConversation = result.data?.createDataHubAiConversation;
                if (newConversation) {
                    setConversationUrn(newConversation.urn);
                    setInitialMessage(messageWithEntity);
                    return true;
                }
                return false;
            } catch {
                // Fallback to main chat page on failure - this ensures users can still ask questions
                // even if sidebar chat initialization fails (GraphQL errors, network issues, etc).
                // Error is intentionally not logged/shown since the fallback provides a seamless
                // working alternative and the user's question is preserved in the navigation state.
                history.push(PageRoutes.AI_CHAT, { initialMessage: messageWithEntity });
                return true; // Navigated to fallback, so input should be cleared
            }
        },
        [entityUrn, entityName, userUrn, createConversation, history],
    );

    return {
        conversationUrn,
        initialMessage,
        isCreating,
        startConversation,
    };
};
