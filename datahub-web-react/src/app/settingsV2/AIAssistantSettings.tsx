import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { Eye } from '@phosphor-icons/react/dist/csr/Eye';
import { EyeSlash } from '@phosphor-icons/react/dist/csr/EyeSlash';

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
    padding: 8px 64px 8px 12px;
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

const InputWrapper = styled.div`
    position: relative;
`;

const VisibilityToggle = styled.button`
    position: absolute;
    top: 50%;
    right: 8px;
    transform: translateY(-50%);
    border: none;
    background: transparent;
    color: ${(props) => props.theme.colors.textSecondary};
    display: inline-flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    padding: 4px;
    line-height: 0;

    &:disabled {
        color: ${(props) => props.theme.colors.textDisabled};
        cursor: not-allowed;
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

const DeleteButton = styled.button`
    padding: 8px 20px;
    background-color: ${(props) => props.theme.colors.bgContainer};
    color: ${(props) => props.theme.colors.text};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 6px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
    &:disabled {
        background-color: ${(props) => props.theme.colors.bgDisabled};
        color: ${(props) => props.theme.colors.textDisabled};
        border-color: ${(props) => props.theme.colors.borderDisabled};
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

type ProviderApiKeyResponse = {
    provider: string;
    hasKey: boolean;
    updated: boolean;
    keyPreview?: string | null;
};

type ProvidersResponse = {
    providers: string[];
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
const BUTTON_LABEL_DELETE = 'Delete Configuration';
const STATUS_SAVED = 'Configuration saved';
const STATUS_DELETED = 'Configuration deleted';
const STATUS_SHORT_KEY = 'API key looks too short';

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
    const [provider, setProvider] = useState('');
    const [apiKey, setApiKey] = useState('');
    const [savedKeyPreview, setSavedKeyPreview] = useState<string | null>(null);
    const [hasSavedKey, setHasSavedKey] = useState(false);
    const [saved, setSaved] = useState(false);
    const [saving, setSaving] = useState(false);
    const [loadingProviders, setLoadingProviders] = useState(true);
    const [loadingProviderState, setLoadingProviderState] = useState(false);
    const [errorMessage, setErrorMessage] = useState<string | null>(null);
    const [showApiKey, setShowApiKey] = useState(false);

    const providerMeta = useMemo(() => getProviderMeta(provider), [provider]);
    let saveButtonLabel = BUTTON_LABEL_SAVE;
    if (hasSavedKey) {
        saveButtonLabel = BUTTON_LABEL_UPDATE;
    }
    if (saving) {
        saveButtonLabel = BUTTON_LABEL_SAVING;
    }

    useEffect(() => {
        let isMounted = true;

        const loadOptions = async () => {
            setLoadingProviders(true);
            setErrorMessage(null);

            try {
                const providersResponse = await fetch(resolveRuntimePath('/api/ai-config/providers'));

                if (!providersResponse.ok) {
                    throw new Error(await getErrorMessage(providersResponse));
                }

                const providersData = (await providersResponse.json()) as ProvidersResponse;

                if (!isMounted) return;

                const nextProviders = (providersData.providers || []).map(toProviderValue);
                setProviders(nextProviders);
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

    const refreshProviderState = async (selectedProvider: string) => {
        const statusResponse = await fetch(
            resolveRuntimePath(
                `/api/ai-config/api-key?${new URLSearchParams({ provider: selectedProvider }).toString()}`,
            ),
        );

        if (!statusResponse.ok) {
            throw new Error(await getErrorMessage(statusResponse));
        }

        const statusData = (await statusResponse.json()) as ProviderApiKeyResponse;
        setHasSavedKey(statusData.hasKey);
        setSavedKeyPreview(statusData.keyPreview || null);
    };

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
            setShowApiKey(false);
            await refreshProviderState(provider);

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

    const handleDelete = async () => {
        if (!provider) return;

        setSaving(true);
        setSaved(false);
        setErrorMessage(null);

        try {
            const response = await fetch(resolveRuntimePath('/api/ai-config/api-key'), {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    provider,
                    apiKey: null,
                }),
            });

            if (!response.ok) {
                throw new Error(await getErrorMessage(response));
            }

            setHasSavedKey(false);
            setSavedKeyPreview(null);
            setApiKey('');
            setShowApiKey(false);
            setSaved(true);

            window.setTimeout(() => setSaved(false), 3000);
        } catch (e) {
            setSaved(false);
            setErrorMessage(
                e instanceof Error ? e.message : 'Failed to delete AI assistant configuration.',
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
                        setShowApiKey(false);
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
                <InputWrapper>
                    <Input
                        id="api-key"
                        type={showApiKey ? 'text' : 'password'}
                        placeholder={providerMeta.keyPrefix}
                        value={apiKey}
                        disabled={!provider || loadingProviderState}
                        onChange={(e) => setApiKey(e.target.value)}
                    />
                    <VisibilityToggle
                        type="button"
                        disabled={!provider || loadingProviderState || apiKey.length === 0}
                        onClick={() => setShowApiKey((current) => !current)}
                        aria-label={showApiKey ? 'Hide API key' : 'Show API key'}
                        title={showApiKey ? 'Hide API key' : 'Show API key'}
                    >
                        {showApiKey ? <EyeSlash size={18} /> : <Eye size={18} />}
                    </VisibilityToggle>
                </InputWrapper>
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
                {hasSavedKey && (
                    <DeleteButton onClick={handleDelete} disabled={saving || !provider}>
                        {BUTTON_LABEL_DELETE}
                    </DeleteButton>
                )}
                {saved && <StatusBadge $success>{hasSavedKey ? STATUS_SAVED : STATUS_DELETED}</StatusBadge>}
                {!isValid && apiKey.length > 0 && (
                    <StatusBadge $success={false}>{STATUS_SHORT_KEY}</StatusBadge>
                )}
                {errorMessage && <StatusBadge $success={false}>{errorMessage}</StatusBadge>}
            </ButtonRow>
        </PageContainer>
    );
};
