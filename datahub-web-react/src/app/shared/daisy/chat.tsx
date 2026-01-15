import React, { useState, useRef, useEffect } from 'react';
import { useChat } from 'ai/react';
import { Button, Input, Card, Typography } from 'antd';
import { MessageOutlined, CloseOutlined, SendOutlined } from '@ant-design/icons';
import styled, { keyframes } from 'styled-components';

// 1. Animation for the Chat Window appearing
const slideUp = keyframes`
  from { opacity: 0; transform: translateY(20px) scale(0.95); }
  to { opacity: 1; transform: translateY(0) scale(1); }
`;

const FloatingContainer = styled.div`
    position: fixed;
    bottom: 40px;
    right: 40px;
    z-index: 1000;
    display: flex;
    flex-direction: column;
    align-items: flex-end;
`;

const ChatWindow = styled(Card)`
    position: absolute;
    bottom: 80px; 
    right: 0;
    width: 380px;
    height: 550px;
    display: flex;
    flex-direction: column;
    box-shadow: 0 12px 48px rgba(0,0,0,0.15);
    border-radius: 16px;
    border: 1px solid #f0f0f0;
    animation: ${slideUp} 0.3s ease-out; /* Add the pop animation */
    
    .ant-card-body { 
        flex: 1; 
        display: flex; 
        flex-direction: column; 
        overflow: hidden; 
        padding: 16px; 
    }
`;

const MessageList = styled.div`
    flex: 1;
    overflow-y: auto;
    margin-bottom: 12px;
    padding-right: 4px;
`;

// 2. Styled Button with nicer hover transitions
const AskDaisyButton = styled(Button)`
    height: 56px !important;
    padding: 0 28px !important;
    font-size: 16px !important;
    font-weight: 600 !important;
    border-radius: 28px !important;
    box-shadow: 0 4px 14px rgba(24, 144, 255, 0.3);
    transition: all 0.3s cubic-bezier(0.175, 0.885, 0.32, 1.275) !important;
    z-index: 1001;

    &:hover {
        transform: translateY(-4px) scale(1.05);
        box-shadow: 0 8px 20px rgba(24, 144, 255, 0.4);
        filter: brightness(1.1);
    }

    &:active {
        transform: translateY(0) scale(0.98);
    }
`;

export const DaisyChat = () => {
  const [isOpen, setIsOpen] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Friendly Welcome Message: Using initialMessages so it's always there
  const { messages, isLoading, input, handleInputChange, handleSubmit } = useChat({
    api: 'https://backoffice.dp.dev.api.discomax.com/daisy/api/chat',
    initialMessages: [
      {
        id: 'welcome',
        role: 'assistant',
        content: "👋 Hi! I'm Daisy, your Beacon data assistant. How can I help you explore your datasets today?"
      },
    ],
    headers: {
      'Authorization': `Bearer ${import.meta.env.REACT_APP_DAISY_TOKEN}`,
      'request-id': `dh-mac-intel-${Date.now()}`,
    },
    body: { domain: 'beacon' },
  });

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  return (
    <FloatingContainer>
      {isOpen && (
        <ChatWindow
          title={<Typography.Text strong style={{ fontSize: '16px' }}>Ask Daisy</Typography.Text>}
          extra={<CloseOutlined onClick={() => setIsOpen(false)} style={{ cursor: 'pointer', color: '#8c8c8c' }} />}
        >
          <MessageList ref={scrollRef}>
            {messages.map((m) => (
              <div key={m.id} style={{ textAlign: m.role === 'user' ? 'right' : 'left', margin: '14px 0' }}>
                <div style={{
                  display: 'inline-block',
                  padding: '12px 16px',
                  borderRadius: m.role === 'user' ? '18px 18px 2px 18px' : '18px 18px 18px 2px',
                  maxWidth: '85%',
                  backgroundColor: m.role === 'user' ? '#1890ff' : '#f5f5f5',
                  color: m.role === 'user' ? 'white' : '#262626',
                  textAlign: 'left',
                  fontSize: '14px',
                  lineHeight: '1.5',
                  boxShadow: '0 2px 8px rgba(0,0,0,0.05)'
                }}>
                  {m.content}
                </div>
              </div>
            ))}
            {isLoading && <Typography.Text type="secondary" style={{ fontSize: '12px', marginLeft: '4px' }}>Daisy is thinking...</Typography.Text>}
          </MessageList>

          <form onSubmit={handleSubmit} style={{ display: 'flex', gap: '8px', borderTop: '1px solid #f0f0f0', paddingTop: '12px' }}>
            <Input
              value={input}
              placeholder="Ask me anything..."
              onChange={handleInputChange}
              disabled={isLoading}
              style={{ borderRadius: '20px' }}
            />
            <Button
              type="primary"
              shape="circle"
              icon={<SendOutlined />}
              htmlType="submit"
              loading={isLoading}
            />
          </form>
        </ChatWindow>
      )}

      <AskDaisyButton
        type="primary"
        icon={<MessageOutlined />}
        onClick={() => setIsOpen(!isOpen)}
      >
        Ask Daisy
      </AskDaisyButton>
    </FloatingContainer>
  );
};