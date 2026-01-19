import {
    CHAT_MODE_OPTIONS,
    ChatMode,
    DEFAULT_CHAT_MODE,
    getAgentNameForChatMode,
    getChatModeForAgentName,
    getModeFromLastAgentMessage,
} from '@app/chat/utils/chatModes';

describe('chatModes', () => {
    describe('CHAT_MODE_OPTIONS', () => {
        it('has three mode options', () => {
            expect(CHAT_MODE_OPTIONS).toHaveLength(3);
        });

        it('includes auto, research, and fast modes', () => {
            const values = CHAT_MODE_OPTIONS.map((opt) => opt.value);
            expect(values).toEqual(['auto', 'research', 'fast']);
        });

        it('has labels for all options', () => {
            CHAT_MODE_OPTIONS.forEach((opt) => {
                expect(opt.label).toBeTruthy();
            });
        });

        it('has descriptions for all options', () => {
            CHAT_MODE_OPTIONS.forEach((opt) => {
                expect(opt.description).toBeTruthy();
            });
        });
    });

    describe('DEFAULT_CHAT_MODE', () => {
        it('is auto', () => {
            expect(DEFAULT_CHAT_MODE).toBe('auto');
        });
    });

    describe('getAgentNameForChatMode', () => {
        it('returns AskDataHubAuto for auto mode', () => {
            expect(getAgentNameForChatMode('auto')).toBe('AskDataHubAuto');
        });

        it('returns AskDataHubResearch for research mode', () => {
            expect(getAgentNameForChatMode('research')).toBe('AskDataHubResearch');
        });

        it('returns AskDataHubFast for fast mode', () => {
            expect(getAgentNameForChatMode('fast')).toBe('AskDataHubFast');
        });
    });

    describe('getChatModeForAgentName', () => {
        it('returns auto for AskDataHubAuto', () => {
            expect(getChatModeForAgentName('AskDataHubAuto')).toBe('auto');
        });

        it('returns research for AskDataHubResearch', () => {
            expect(getChatModeForAgentName('AskDataHubResearch')).toBe('research');
        });

        it('returns fast for AskDataHubFast', () => {
            expect(getChatModeForAgentName('AskDataHubFast')).toBe('fast');
        });

        it('returns default mode for null', () => {
            expect(getChatModeForAgentName(null)).toBe(DEFAULT_CHAT_MODE);
        });

        it('returns default mode for undefined', () => {
            expect(getChatModeForAgentName(undefined)).toBe(DEFAULT_CHAT_MODE);
        });

        it('returns default mode for unknown agent name', () => {
            expect(getChatModeForAgentName('UnknownAgent')).toBe(DEFAULT_CHAT_MODE);
        });

        it('returns default mode for empty string', () => {
            expect(getChatModeForAgentName('')).toBe(DEFAULT_CHAT_MODE);
        });
    });

    describe('bidirectional mapping', () => {
        it('round-trips auto mode', () => {
            const mode: ChatMode = 'auto';
            const agentName = getAgentNameForChatMode(mode);
            expect(getChatModeForAgentName(agentName)).toBe(mode);
        });

        it('round-trips research mode', () => {
            const mode: ChatMode = 'research';
            const agentName = getAgentNameForChatMode(mode);
            expect(getChatModeForAgentName(agentName)).toBe(mode);
        });

        it('round-trips fast mode', () => {
            const mode: ChatMode = 'fast';
            const agentName = getAgentNameForChatMode(mode);
            expect(getChatModeForAgentName(agentName)).toBe(mode);
        });
    });

    describe('getModeFromLastAgentMessage', () => {
        it('returns default mode for undefined messages', () => {
            expect(getModeFromLastAgentMessage(undefined)).toBe(DEFAULT_CHAT_MODE);
        });

        it('returns default mode for null messages', () => {
            expect(getModeFromLastAgentMessage(null)).toBe(DEFAULT_CHAT_MODE);
        });

        it('returns default mode for empty array', () => {
            expect(getModeFromLastAgentMessage([])).toBe(DEFAULT_CHAT_MODE);
        });

        it('returns default mode when no messages have agentName', () => {
            const messages = [{ agentName: null }, { agentName: undefined }, {}];
            expect(getModeFromLastAgentMessage(messages)).toBe(DEFAULT_CHAT_MODE);
        });

        it('returns mode from last message with agentName', () => {
            const messages = [
                { agentName: 'AskDataHubAuto' },
                { agentName: 'AskDataHubResearch' },
                { agentName: null },
            ];
            expect(getModeFromLastAgentMessage(messages)).toBe('research');
        });

        it('returns mode from single message with agentName', () => {
            const messages = [{ agentName: 'AskDataHubFast' }];
            expect(getModeFromLastAgentMessage(messages)).toBe('fast');
        });

        it('skips messages without agentName and finds earlier one', () => {
            const messages = [{ agentName: 'AskDataHubResearch' }, { agentName: null }, { agentName: undefined }];
            expect(getModeFromLastAgentMessage(messages)).toBe('research');
        });

        it('returns default mode for unknown agent name', () => {
            const messages = [{ agentName: 'UnknownAgent' }];
            expect(getModeFromLastAgentMessage(messages)).toBe(DEFAULT_CHAT_MODE);
        });
    });
});
