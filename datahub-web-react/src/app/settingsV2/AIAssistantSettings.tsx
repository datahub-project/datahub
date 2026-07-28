import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { resolveRuntimePath } from '@utils/runtimeBasePath';

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
    color: ${(props) => props.theme.colors.textSecondary};
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
    color: ${(props) => props.theme.colors.textTertiary};
    margin-top: 4px;
    margin-bottom: 0;
`;

const Input = styled.input`
    width: 100%;
    padding: 8px 12px;
    border: 1px solid ${(props) => props.theme.colors.borderInput};
    border-radius: 6px;
    font-size: 14px;
    font-family: monospace;
    box-sizing: border-box;
    background: ${(props) => props.theme.colors.bgInput};
    color: ${(props) => props.theme.colors.text};
    &:focus {
        outline: none;
        border-color: ${(props) => props.theme.colors.borderInputFocus};
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};
    }
`;

const Select = styled.select`
    width: 100%;
    padding: 8px 12px;
    border: 1px solid ${(props) => props.theme.colors.borderInput};
    border-radius: 6px;
    font-size: 14px;
    background: ${(props) => props.theme.colors.bgInput};
    color: ${(props) => props.theme.colors.text};
    box-sizing: border-box;
    &:focus {
        outline: none;
        border-color: ${(props) => props.theme.colors.borderInputFocus};
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};
    }
`;

const ButtonRow = styled.div`
    display: flex;
    gap: 12px;
    margin-top: 8px;
    align-items: center;
    flex-wrap: wrap;
`;

const SaveButton = styled.button`
    padding: 8px 20px;
    background-color: ${(props) => props.theme.colors.buttonFillBrand};
    color: ${(props) => props.theme.colors.textOnFillBrand};
    border: none;
    border-radius: 6px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    &:hover {
        background-color: ${(props) => props.theme.colors.buttonSurfaceBrandHover};
    }
    &:disabled {
        background-color: ${(props) => props.theme.colors.bgDisabled};
        color: ${(props) => props.theme.colors.textDisabled};
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
    background-color: ${(props) =>
        props.$success ? props.theme.colors.bgSurfaceSuccess : props.theme.colors.bgSurfaceError};
    color: ${(props) =>
        props.$success ? props.theme.colors.textOnSurfaceSuccess : props.theme.colors.textOnSurfaceError};
    border: 1px solid
        ${(props) => (props.$success ? props.theme.colors.borderSuccess : props.theme.colors.borderError)};
`;

const Divider = styled.hr`
    border: none;
    border-top: 1px solid ${(props) => props.theme.colors.border};
    margin: 28px 0;
`;

const InfoBox = styled.div`
    background: ${(props) => props.theme.colors.bgSurfaceInfo};
    border: 1px solid ${(props) => props.theme.colors.borderInformation};
    border-radius: 8px;
    padding: 14px 16px;
    font-size: 13px;
    color: ${(props) => props.theme.colors.textOnSurfaceInformation};
    line-height: 1.5;
`;

type ProviderApiKeyResponse = {
    provider: string;
    hasKey: boolean;
    updated: boolean;
    keyPreview?: string | null;
};

type ProvidersResponse = {
    providers: string[];
};

type ModelsResponse = {
    models: string[];
};

type ErrorResponse = {
    error?: string;
};

type ProviderMeta = {
    label: string;
    keyPrefix: string;
    consoleUrl: string;
    consoleName: string;
};

const PROVIDER_METADATA: Record<string, ProviderMeta> = {
    claude: {
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

const MODEL_LABELS: Record<string, string> = {
    SONNET: 'Claude Sonnet',
    OPUS: 'Claude Opus',
    GPT_5_5: 'GPT 5.5',
};

const DEFAULT_PROVIDER_META: ProviderMeta = {
    label: 'Provider',
    keyPrefix: 'api-key',
    consoleUrl: '#',
    consoleName: 'provider console',
};

const PAGE_TITLE = 'AI Assistant';
const PAGE_SUBTITLE =
    'Configure an LLM provider to power the DataHub AI Assistant. Once configured, users can ask questions about datasets, schemas, lineage, and privacy risk directly from any DataHub page.';
const PROVIDER_LABEL = 'Provider';
const API_KEY_LABEL = 'API Key';
const PROVIDER_LOADING_TEXT = 'Loading providers from GMS...';
const PROVIDER_LOADED_TEXT = 'Available providers are served by the backend configuration API.';
const API_KEY_HELP_PREFIX = 'Get your API key from';
const API_KEY_HELP_SUFFIX =
    ". The key is encrypted by DataHub's secret service, stored in the backend, and never exposed to end users or sent on each chat request.";
const API_KEY_STATUS_LOADING = 'Checking whether this provider already has a saved key...';
const API_KEY_STATUS_SAVED_PREFIX = 'Saved key on file:';
const API_KEY_STATUS_EXISTS = 'A saved key already exists for this provider.';
const BUTTON_LABEL_SAVING = 'Saving...';
const BUTTON_LABEL_UPDATE = 'Update Configuration';
const BUTTON_LABEL_SAVE = 'Save Configuration';
const STATUS_SAVED = 'Configuration saved';
const STATUS_SHORT_KEY = 'API key looks too short';
const MODEL_SELECTION_TITLE = 'Model selection has moved.';
const MODEL_SELECTION_EMPTY = 'Pick the model in the chat panel.';
const MODEL_SELECTION_SUFFIX =
    'Pick the model per conversation in the chat panel instead of changing it here.';

const toProviderValue = (provider: string) => provider.toLowerCase();

const getProviderMeta = (provider: string): ProviderMeta =>
    PROVIDER_METADATA[provider] || {
        ...DEFAULT_PROVIDER_META,
        label: provider.toUpperCase(),
    };

const getErrorMessage = async (response: Response): Promise<string> => {
    try {
        const data = (await response.json()) as ErrorResponse;
        return data.error || `Request failed with status ${response.status}`;
    } catch {
        return `Request failed with status ${response.status}`;
    }
};

export const AIAssistantSettings = () => {
    const [providers, setProviders] = useState<string[]>([]);
    const [models, setModels] = useState<string[]>([]);
    const [provider, setProvider] = useState('');
    const [apiKey, setApiKey] = useState('');
    const [savedKeyPreview, setSavedKeyPreview] = useState<string | null>(null);
    const [hasSavedKey, setHasSavedKey] = useState(false);
    const [saved, setSaved] = useState(false);
    const [saving, setSaving] = useState(false);
    const [loadingProviders, setLoadingProviders] = useState(true);
    const [loadingProviderState, setLoadingProviderState] = useState(false);
    const [errorMessage, setErrorMessage] = useState<string | null>(null);

    const providerMeta = useMemo(() => getProviderMeta(provider), [provider]);
    let saveButtonLabel = BUTTON_LABEL_SAVE;
    if (hasSavedKey) {
        saveButtonLabel = BUTTON_LABEL_UPDATE;
    }
    if (saving) {
        saveButtonLabel = BUTTON_LABEL_SAVING;
    }
    const modelSummaryText =
        models.length > 0
            ? `Supported models from GMS: ${models
                  .map((model) => MODEL_LABELS[model] || model.replaceAll('_', ' '))
                  .join(', ')}.`
            : MODEL_SELECTION_EMPTY;

    useEffect(() => {
        let isMounted = true;

        const loadOptions = async () => {
            setLoadingProviders(true);
            setErrorMessage(null);

            try {
                const [providersResponse, modelsResponse] = await Promise.all([
                    fetch(resolveRuntimePath('/api/ai-config/providers')),
                    fetch(resolveRuntimePath('/api/ai-config/models')),
                ]);

                if (!providersResponse.ok) {
                    throw new Error(await getErrorMessage(providersResponse));
                }

                if (!modelsResponse.ok) {
                    throw new Error(await getErrorMessage(modelsResponse));
                }

                const providersData = (await providersResponse.json()) as ProvidersResponse;
                const modelsData = (await modelsResponse.json()) as ModelsResponse;

                if (!isMounted) return;

                const nextProviders = (providersData.providers || []).map(toProviderValue);
                setProviders(nextProviders);
                setModels(modelsData.models || []);
                setProvider((currentProvider) => currentProvider || nextProviders[0] || '');
            } catch (e) {
                if (!isMounted) return;
                setErrorMessage(
                    e instanceof Error ? e.message : 'Failed to load AI assistant settings.',
                );
            } finally {
                if (isMounted) {
                    setLoadingProviders(false);
                }
            }
        };

        loadOptions();

        return () => {
            isMounted = false;
        };
    }, []);

    useEffect(() => {
        let isMounted = true;

        const loadProviderState = async () => {
            if (!provider) return;

            setLoadingProviderState(true);
            setErrorMessage(null);

            try {
                const query = new URLSearchParams({ provider });
                const response = await fetch(
                    resolveRuntimePath(`/api/ai-config/api-key?${query.toString()}`),
                );

                if (!response.ok) {
                    throw new Error(await getErrorMessage(response));
                }

                const data = (await response.json()) as ProviderApiKeyResponse;

                if (!isMounted) return;

                setHasSavedKey(data.hasKey);
                setSavedKeyPreview(data.keyPreview || null);
            } catch (e) {
                if (!isMounted) return;
                setHasSavedKey(false);
                setSavedKeyPreview(null);
                setErrorMessage(
                    e instanceof Error ? e.message : 'Failed to load provider key status.',
                );
            } finally {
                if (isMounted) {
                    setLoadingProviderState(false);
                }
            }
        };

        loadProviderState();

        return () => {
            isMounted = false;
        };
    }, [provider]);

    const handleSave = async () => {
        if (!provider || !apiKey.trim()) return;

        setSaving(true);
        setErrorMessage(null);

        try {
            const response = await fetch(resolveRuntimePath('/api/ai-config/api-key'), {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    provider,
                    apiKey: apiKey.trim(),
                }),
            });

            if (!response.ok) {
                throw new Error(await getErrorMessage(response));
            }

            const data = (await response.json()) as ProviderApiKeyResponse;
            setHasSavedKey(data.hasKey);
            setSaved(true);
            setApiKey('');

            const statusResponse = await fetch(
                resolveRuntimePath(
                    `/api/ai-config/api-key?${new URLSearchParams({ provider }).toString()}`,
                ),
            );

            if (statusResponse.ok) {
                const statusData = (await statusResponse.json()) as ProviderApiKeyResponse;
                setSavedKeyPreview(statusData.keyPreview || null);
            }

            window.setTimeout(() => setSaved(false), 3000);
        } catch (e) {
            setSaved(false);
            setErrorMessage(
                e instanceof Error ? e.message : 'Failed to save AI assistant configuration.',
            );
        } finally {
            setSaving(false);
        }
    };

    const isValid = apiKey.trim().length > 10;

    return (
        <PageContainer>
            <PageTitle>{PAGE_TITLE}</PageTitle>
            <PageSubtitle>{PAGE_SUBTITLE}</PageSubtitle>

            <Divider />

            <Section>
                <Label htmlFor="provider">{PROVIDER_LABEL}</Label>
                <Select
                    id="provider"
                    value={provider}
                    disabled={loadingProviders || providers.length === 0}
                    onChange={(e) => {
                        setProvider(e.target.value);
                        setApiKey('');
                        setSaved(false);
                        setErrorMessage(null);
                    }}
                >
                    {providers.map((value) => {
                        const meta = getProviderMeta(value);
                        return (
                            <option key={value} value={value}>
                                {meta.label}
                            </option>
                        );
                    })}
                </Select>
                <HelpText>
                    {loadingProviders ? PROVIDER_LOADING_TEXT : PROVIDER_LOADED_TEXT}
                </HelpText>
            </Section>

            <Section>
                <Label htmlFor="api-key">{API_KEY_LABEL}</Label>
                <Input
                    id="api-key"
                    type="password"
                    placeholder={providerMeta.keyPrefix}
                    value={apiKey}
                    disabled={!provider || loadingProviderState}
                    onChange={(e) => setApiKey(e.target.value)}
                />
                <HelpText>
                    {API_KEY_HELP_PREFIX}{' '}
                    <a href={providerMeta.consoleUrl} target="_blank" rel="noreferrer">
                        {providerMeta.consoleName}
                    </a>
                    {API_KEY_HELP_SUFFIX}
                </HelpText>
                {loadingProviderState && <HelpText>{API_KEY_STATUS_LOADING}</HelpText>}
                {!loadingProviderState && hasSavedKey && savedKeyPreview && (
                    <HelpText>
                        {API_KEY_STATUS_SAVED_PREFIX} {savedKeyPreview}
                    </HelpText>
                )}
                {!loadingProviderState && hasSavedKey && !savedKeyPreview && (
                    <HelpText>{API_KEY_STATUS_EXISTS}</HelpText>
                )}
            </Section>

            <ButtonRow>
                <SaveButton onClick={handleSave} disabled={!isValid || saving || !provider}>
                    {saveButtonLabel}
                </SaveButton>
                {saved && <StatusBadge $success>{STATUS_SAVED}</StatusBadge>}
                {!isValid && apiKey.length > 0 && (
                    <StatusBadge $success={false}>{STATUS_SHORT_KEY}</StatusBadge>
                )}
                {errorMessage && <StatusBadge $success={false}>{errorMessage}</StatusBadge>}
            </ButtonRow>

            <Divider />

            <InfoBox>
                <strong>{MODEL_SELECTION_TITLE}</strong> {modelSummaryText}{' '}
                {MODEL_SELECTION_SUFFIX}
            </InfoBox>
        </PageContainer>
    );
};
