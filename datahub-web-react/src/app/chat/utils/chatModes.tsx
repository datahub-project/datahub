import { GraduationCap, Lightning, Sparkle } from '@phosphor-icons/react';
import React from 'react';

import { SelectOption } from '@components/components/Select/types';

export type ChatMode = 'auto' | 'research' | 'fast';

export const CHAT_MODE_OPTIONS: SelectOption[] = [
    {
        value: 'auto',
        label: 'Auto',
        description: 'Best for everyday tasks',
        icon: <Sparkle size={14} weight="regular" />,
    },
    {
        value: 'research',
        label: 'Research',
        description: 'Longer reasoning for more complex tasks',
        icon: <GraduationCap size={14} weight="regular" />,
    },
    {
        value: 'fast',
        label: 'Fast',
        description: 'Faster responses with less thinking',
        icon: <Lightning size={14} weight="regular" />,
    },
];

const CHAT_MODE_TO_AGENT_NAME: Record<ChatMode, string> = {
    auto: 'AskDataHubAuto',
    research: 'AskDataHubResearch',
    fast: 'AskDataHubFast',
};

export const getAgentNameForChatMode = (mode: ChatMode): string => {
    return CHAT_MODE_TO_AGENT_NAME[mode];
};

const AGENT_NAME_TO_CHAT_MODE: Record<string, ChatMode> = Object.fromEntries(
    Object.entries(CHAT_MODE_TO_AGENT_NAME).map(([mode, agentName]) => [agentName, mode as ChatMode]),
) as Record<string, ChatMode>;

export const DEFAULT_CHAT_MODE: ChatMode = 'auto';

export const getChatModeForAgentName = (agentName: string | null | undefined): ChatMode => {
    if (!agentName) return DEFAULT_CHAT_MODE;
    return AGENT_NAME_TO_CHAT_MODE[agentName] ?? DEFAULT_CHAT_MODE;
};

/**
 * Finds the chat mode from the last message that has an agentName.
 * Returns DEFAULT_CHAT_MODE if no message has an agentName.
 */
export const getModeFromLastAgentMessage = (
    messages: Array<{ agentName?: string | null }> | undefined | null,
): ChatMode => {
    if (!messages || messages.length === 0) return DEFAULT_CHAT_MODE;

    for (let i = messages.length - 1; i >= 0; i--) {
        const { agentName } = messages[i];
        if (agentName) {
            return getChatModeForAgentName(agentName);
        }
    }

    return DEFAULT_CHAT_MODE;
};
