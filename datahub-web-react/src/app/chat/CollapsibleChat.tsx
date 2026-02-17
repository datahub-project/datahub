import { Icon } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ChatLocationType } from '@app/analytics';
import { EmbeddedChat } from '@app/chat/EmbeddedChat';
import { MessageContext } from '@app/chat/hooks/useChatStream';

import { DataHubAiConversationOriginType } from '@types';

const Container = styled.div`
    display: flex;
    height: 100%;
    flex-direction: column;
`;

const CollapsibleChatHeader = styled.div`
    display: flex;
    width: 100%;
    justify-content: flex-end;
    padding: 8px 12px;
`;

const RightSection = styled.div`
    display: flex;
`;

const StyledIcon = styled(Icon)`
    :hover {
        cursor: pointer;
    }
`;

interface Props {
    setIsChatOpen: React.Dispatch<React.SetStateAction<boolean>>;
    context?: string;
    agentName?: string;
    originType: DataHubAiConversationOriginType;
    title?: string;
    contentPlaceholder?: string;
    getMessageContext?: () => MessageContext;
    chatLocation: ChatLocationType;
    suggestedQuestions?: string[];
}

export default function CollapsibleChat({
    setIsChatOpen,
    context,
    agentName,
    originType,
    title,
    contentPlaceholder,
    getMessageContext,
    chatLocation,
    suggestedQuestions,
}: Props) {
    const minimizeChat = () => {
        setIsChatOpen(false);
    };

    return (
        <Container>
            <CollapsibleChatHeader>
                <RightSection>
                    <StyledIcon icon="Minus" source="phosphor" color="gray" size="3xl" onClick={minimizeChat} />
                </RightSection>
            </CollapsibleChatHeader>
            <EmbeddedChat
                context={context}
                agentName={agentName}
                originType={originType}
                title={title}
                chatLocation={chatLocation}
                getMessageContext={getMessageContext}
                contentPlaceholder={contentPlaceholder}
                suggestedQuestions={suggestedQuestions}
            />
        </Container>
    );
}
