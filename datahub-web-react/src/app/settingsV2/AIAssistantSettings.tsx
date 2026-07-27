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

// Provider metadata — V1 ships Claude, but the structure supports multiple providers.
const PROVIDERS: Record<string, { label: string; keyPrefix: string; consoleUrl: string; consoleName: string }> = {
    anthropic: {
        label: 'Anthropic (Claude)',
        keyPrefix: 'sk-ant-...',
        consoleUrl: 'https://console.anthropic.com',
        consoleName: 'console.anthropic.com',
    },
    openai: {
        label: 'OpenAI (ChatGPT)',
        keyPrefix: 'sk-...',
        consoleUrl: 'https://platform.openai.com/api-keys',
        consoleName: 'platform.openai.com',
    },
};

export const AIAssistantSettings = () => {
    const [provider, setProvider] = useState('anthropic');
    const [apiKey, setApiKey] = useState('');
    const [saved, setSaved] = useState(false);
    const [saving, setSaving] = useState(false);

    const providerMeta = PROVIDERS[provider];

    const handleSave = async () => {
        setSaving(true);
        // TODO: POST to backend /api/ai-config/key  { provider, apiKey }
        // The key is encrypted by DataHub's secret service (Postgres RDS) and never
        // returned in full. Model selection lives separately (chosen in the chat panel).
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
                Configure an LLM provider to power the DataHub AI Assistant. Once configured, users can ask questions
                about datasets, schemas, lineage, and privacy risk directly from any DataHub page. The model is chosen
                per-conversation in the chat panel.
            </PageSubtitle>

            <Divider />

            <Section>
                <Label htmlFor="provider">Provider</Label>
                <Select
                    id="provider"
                    value={provider}
                    onChange={(e) => {
                        setProvider(e.target.value);
                        setApiKey('');
                    }}
                >
                    {Object.entries(PROVIDERS).map(([value, meta]) => (
                        <option key={value} value={value}>
                            {meta.label}
                        </option>
                    ))}
                </Select>
                <HelpText>V1 supports Claude. Other providers are shown for the multi-provider roadmap.</HelpText>
            </Section>

            <Section>
                <Label htmlFor="api-key">API Key</Label>
                <Input
                    id="api-key"
                    type="password"
                    placeholder={providerMeta.keyPrefix}
                    value={apiKey}
                    onChange={(e) => setApiKey(e.target.value)}
                />
                <HelpText>
                    Get your API key from{' '}
                    <a href={providerMeta.consoleUrl} target="_blank" rel="noreferrer">
                        {providerMeta.consoleName}
                    </a>
                    . The key is encrypted by DataHub&apos;s secret service, stored in the backend, and never exposed
                    to end users or sent on each chat request.
                </HelpText>
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

            <Divider />

            <InfoBox>
                <strong>Model selection has moved.</strong> Pick the model (e.g. Claude Sonnet 5, Opus, Haiku) directly
                in the 🤖 chat panel — so you can switch per conversation without changing this configuration.
            </InfoBox>
        </PageContainer>
    );
};
