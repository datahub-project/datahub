import React, { useState } from 'react';
import styled from 'styled-components';

const PageContainer = styled.div`
    padding: 32px 40px;
    max-width: 640px;
`;

const PageTitle = styled.h2`
    font-size: 20px;
    font-weight: 700;
    margin-bottom: 4px;
`;

const PageSubtitle = styled.p`
    font-size: 14px;
    color: #666;
    margin-bottom: 32px;
`;

const Section = styled.div`
    margin-bottom: 28px;
`;

const Label = styled.label`
    display: block;
    font-size: 14px;
    font-weight: 600;
    margin-bottom: 6px;
`;

const HelpText = styled.p`
    font-size: 12px;
    color: #888;
    margin-top: 4px;
    margin-bottom: 0;
`;

const Input = styled.input`
    width: 100%;
    padding: 8px 12px;
    border: 1px solid #d9d9d9;
    border-radius: 6px;
    font-size: 14px;
    font-family: monospace;
    box-sizing: border-box;
    &:focus {
        outline: none;
        border-color: #7b5ea7;
        box-shadow: 0 0 0 2px rgba(123, 94, 167, 0.15);
    }
`;

const Select = styled.select`
    width: 100%;
    padding: 8px 12px;
    border: 1px solid #d9d9d9;
    border-radius: 6px;
    font-size: 14px;
    background: white;
    box-sizing: border-box;
    &:focus {
        outline: none;
        border-color: #7b5ea7;
        box-shadow: 0 0 0 2px rgba(123, 94, 167, 0.15);
    }
`;

const ButtonRow = styled.div`
    display: flex;
    gap: 12px;
    margin-top: 8px;
`;

const SaveButton = styled.button`
    padding: 8px 20px;
    background-color: #7b5ea7;
    color: white;
    border: none;
    border-radius: 6px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    &:hover {
        background-color: #6a4e90;
    }
    &:disabled {
        background-color: #ccc;
        cursor: not-allowed;
    }
`;

const StatusBadge = styled.span<{ $success: boolean }>`
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 6px 14px;
    border-radius: 20px;
    font-size: 13px;
    font-weight: 500;
    background-color: ${(props) => (props.$success ? '#f0fdf4' : '#fef2f2')};
    color: ${(props) => (props.$success ? '#16a34a' : '#dc2626')};
    border: 1px solid ${(props) => (props.$success ? '#bbf7d0' : '#fecaca')};
`;

const Divider = styled.hr`
    border: none;
    border-top: 1px solid #f0f0f0;
    margin: 28px 0;
`;

const InfoBox = styled.div`
    background: #f8f4ff;
    border: 1px solid #e0d5f5;
    border-radius: 8px;
    padding: 14px 16px;
    font-size: 13px;
    color: #5a4080;
    line-height: 1.5;
`;

export const AIAssistantSettings = () => {
    const [apiKey, setApiKey] = useState('');
    const [model, setModel] = useState('claude-sonnet-5');
    const [saved, setSaved] = useState(false);
    const [saving, setSaving] = useState(false);

    const handleSave = async () => {
        setSaving(true);
        // TODO: POST to orchestrator /api/ai-config
        // For now, simulate save
        await new Promise((resolve) => setTimeout(resolve, 800));
        setSaved(true);
        setSaving(false);
        setTimeout(() => setSaved(false), 3000);
    };

    const isValid = apiKey.trim().length > 10;

    return (
        <PageContainer>
            <PageTitle>🤖 AI Assistant</PageTitle>
            <PageSubtitle>
                Configure Claude to power the DataHub AI Assistant. Once configured, users can ask questions about
                datasets, schemas, lineage, and privacy risk directly from any DataHub page.
            </PageSubtitle>

            <Divider />

            <Section>
                <Label htmlFor="api-key">Claude API Key</Label>
                <Input
                    id="api-key"
                    type="password"
                    placeholder="sk-ant-..."
                    value={apiKey}
                    onChange={(e) => setApiKey(e.target.value)}
                />
                <HelpText>
                    Get your API key from{' '}
                    <a href="https://console.anthropic.com" target="_blank" rel="noreferrer">
                        console.anthropic.com
                    </a>
                    . This key is stored securely and never exposed to end users.
                </HelpText>
            </Section>

            <Section>
                <Label htmlFor="model">Model</Label>
                <Select id="model" value={model} onChange={(e) => setModel(e.target.value)}>
                    <option value="claude-sonnet-5">Claude Sonnet 5 (Recommended — best balance of speed &amp; cost)</option>
                    <option value="claude-opus-4-8">Claude Opus 4.8 (Most capable — complex reasoning)</option>
                    <option value="claude-haiku-4-5">Claude Haiku 4.5 (Fastest &amp; cheapest)</option>
                </Select>
                <HelpText>Claude Sonnet 5 is recommended for data governance tasks — strong reasoning at lower cost than Opus.</HelpText>
            </Section>

            <ButtonRow>
                <SaveButton onClick={handleSave} disabled={!isValid || saving}>
                    {saving ? 'Saving...' : 'Save Configuration'}
                </SaveButton>
                {saved && <StatusBadge $success>✓ Configuration saved</StatusBadge>}
                {!isValid && apiKey.length > 0 && (
                    <StatusBadge $success={false}>⚠ API key looks too short</StatusBadge>
                )}
            </ButtonRow>
        </PageContainer>
    );
};
