import { green } from '@ant-design/colors';
import { InfoCircleFilled } from '@ant-design/icons';
import { Alert, Divider, Form, Image, Input, Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { PlatformIntegrationBreadcrumb } from '@app/settingsV2/platform/PlatformIntegrationBreadcrumb';
import { TEAMS_CONNECTION_URN } from '@app/settingsV2/platform/teams/utils';
import { createOAuthUrl } from '@app/settingsV2/teams/utils/oauthState';
import { Button } from '@src/alchemy-components';
import { getRuntimeBasePath } from '@utils/runtimeBasePath';

import { useConnectionQuery } from '@graphql/connection.generated';
import { useGetTeamsOAuthConfigQuery } from '@graphql/teams.generated';

import teamsLogo from '@images/teamslogo.png';

const Page = styled.div`
    width: 100%;
    display: flex;
    justify-content: center;
`;

const ContentContainer = styled.div`
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
    width: 100%;
`;

const Content = styled.div`
    display: flex;
    align-items: top;
    justify-content: space-between;
`;

const FormColumn = styled.div``;

const SettingValueContainer = styled.div`
    margin-top: 12px;
`;

const StyledInput = styled(Input)`
    width: 240px;
`;

const PlatformLogo = styled(Image)`
    max-height: 16px;
    margin-right: 8px;
    margin-bottom: 2px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const InfoIcon = styled(InfoCircleFilled)`
    color: ${green[6]};
    margin-right: 8px;
`;

const GlobalNotificationsBanner = styled.div`
    background-color: ${green[0]};
    border-radius: 8px;
    border: 1px solid ${green[6]};
    padding: 8px 16px;
    margin: 18px 0 25px;
    font-size: 14px;
`;

interface TeamsConnection {
    tenant_id?: string;
    teams_url?: string;
}

const DEFAULT_CONNECTION: TeamsConnection = {
    tenant_id: '',
    teams_url: '',
};

const decodeTeamsConnection = (jsonString: string): TeamsConnection => {
    try {
        const parsed = JSON.parse(jsonString);
        return {
            tenant_id: parsed.app_details?.tenant_id || '',
            teams_url: '', // Not stored in backend, UI-only field
        };
    } catch {
        return DEFAULT_CONNECTION;
    }
};

const extractTenantIdFromTeamsUrl = (url: string): string | null => {
    if (!url) return null;

    try {
        // Handle different Teams URL formats that contain tenantId parameter
        const urlObj = new URL(url);

        // Check for tenantId in URL parameters
        const tenantId = urlObj.searchParams.get('tenantId');
        if (tenantId) {
            return tenantId;
        }

        // Check for tenantId in hash parameters (some Teams URLs use hash routing)
        const { hash } = urlObj;
        if (hash) {
            const hashParams = new URLSearchParams(hash.substring(1));
            const hashTenantId = hashParams.get('tenantId');
            if (hashTenantId) {
                return hashTenantId;
            }
        }

        return null;
    } catch {
        return null;
    }
};

export const TeamsIntegration = () => {
    const [connection, setConnection] = useState<TeamsConnection>(DEFAULT_CONNECTION);
    const [urlExtractionStatus, setUrlExtractionStatus] = useState<'idle' | 'success' | 'error'>('idle');

    const {
        data: connData,
        loading: connLoading,
        error,
    } = useConnectionQuery({
        variables: {
            urn: TEAMS_CONNECTION_URN,
        },
    });

    // Fetch OAuth configuration using GraphQL
    const {
        data: oauthConfigData,
        loading: oauthConfigLoading,
        error: oauthConfigError,
    } = useGetTeamsOAuthConfigQuery();

    const existingConnJson = connData?.connection?.details?.json;
    const teamsConnData = existingConnJson && decodeTeamsConnection(existingConnJson.blob as string);
    const isConnected = teamsConnData?.tenant_id;

    useEffect(() => {
        if (teamsConnData && connection === DEFAULT_CONNECTION) {
            setConnection(teamsConnData);
        }
    }, [teamsConnData, connection]);

    // Handle OAuth callback results from URL parameters
    useEffect(() => {
        const urlParams = new URLSearchParams(window.location.search);
        const oauthResult = urlParams.get('oauth');
        const errorMessage = urlParams.get('message');

        if (oauthResult === 'success') {
            message.success('Teams integration configured successfully! Your Teams bot is now ready to use.');
            // Clean up URL parameters
            const newUrl = window.location.pathname;
            window.history.replaceState({}, document.title, newUrl);
        } else if (oauthResult === 'error') {
            const displayMessage = errorMessage
                ? decodeURIComponent(errorMessage)
                : 'OAuth configuration failed. Please try again.';
            message.error(displayMessage);
            // Clean up URL parameters
            const newUrl = window.location.pathname;
            window.history.replaceState({}, document.title, newUrl);
        }
    }, []); // Run once on component mount

    const generateOAuthUrl = (tenantId: string, instanceUrl: string) => {
        const oauthConfig = oauthConfigData?.teamsOAuthConfig;
        if (!oauthConfig) {
            console.error('OAuth configuration not available');
            return '';
        }

        // Use shared utility to create OAuth URL
        return createOAuthUrl(
            {
                url: instanceUrl,
                flowType: 'platform_integration',
                tenantId,
                redirectPath: '/settings/integrations/microsoft-teams',
            },
            {
                appId: oauthConfig.appId,
                redirectUri: oauthConfig.redirectUri,
                scopes: oauthConfig.scopes,
                baseAuthUrl: oauthConfig.baseAuthUrl,
            },
            tenantId,
        );
    };

    const startOAuthFlow = () => {
        if (!connection.tenant_id) {
            message.error('Please enter a Teams URL to extract the tenant ID first.');
            return;
        }

        if (oauthConfigLoading) {
            message.info('Loading OAuth configuration...');
            return;
        }

        if (oauthConfigError || !oauthConfigData?.teamsOAuthConfig) {
            message.error('Failed to load OAuth configuration. Please try again.');
            return;
        }

        // Get current DataHub instance URL
        const instanceUrl = window.location.origin + getRuntimeBasePath();

        // Generate OAuth URL and redirect
        const oauthUrl = generateOAuthUrl(connection.tenant_id, instanceUrl);

        if (!oauthUrl) {
            message.error('Failed to generate OAuth URL. Please check the configuration.');
            return;
        }

        message.info('Redirecting to Microsoft for OAuth authorization...');

        // Redirect to OAuth flow
        window.location.href = oauthUrl;
    };

    const handleTeamsUrlChange = (url: string) => {
        setConnection({ ...connection, teams_url: url });

        if (!url.trim()) {
            setConnection({ ...connection, teams_url: '', tenant_id: '' });
            setUrlExtractionStatus('idle');
            return;
        }

        const extractedTenantId = extractTenantIdFromTeamsUrl(url);
        if (extractedTenantId) {
            setConnection({ ...connection, teams_url: url, tenant_id: extractedTenantId });
            setUrlExtractionStatus('success');
        } else {
            setConnection({ ...connection, teams_url: url, tenant_id: '' });
            setUrlExtractionStatus('error');
        }
    };

    const isFormValid = connection.tenant_id;

    return (
        <Page>
            <ContentContainer>
                {error && <Alert type="error" message={error?.message || `Failed to load Teams connection`} />}
                <PlatformIntegrationBreadcrumb name="Teams" />
                <Typography.Title level={3}>Teams</Typography.Title>
                <Typography.Text type="secondary">Setup Microsoft Teams Integration</Typography.Text>
                <Divider />
                {isConnected ? (
                    <GlobalNotificationsBanner>
                        <InfoIcon />
                        The Teams integration is ready! Now switch to the{' '}
                        <Link to="/settings/notifications">Platform Notifications Tab</Link> to try it out.
                    </GlobalNotificationsBanner>
                ) : (
                    !connLoading && (
                        <Alert
                            type="info"
                            message="Teams integration is not configured"
                            description="Paste a Teams URL to automatically extract your tenant ID and enable Teams integration."
                            style={{ marginBottom: 16 }}
                        />
                    )
                )}
                <Content>
                    <FormColumn>
                        <Form layout="vertical">
                            <Form.Item required label={<Typography.Text strong>Enter Teams URL</Typography.Text>}>
                                <Typography.Text type="secondary">
                                    In Microsoft Teams, click the &apos;...&apos; next to your team name and choose
                                    &apos;Get link to team&apos;.
                                </Typography.Text>
                                <SettingValueContainer>
                                    <StyledInput
                                        value={connection.teams_url || ''}
                                        placeholder="https://teams.microsoft.com/l/team/..."
                                        data-testid="teams-url-input"
                                        onChange={(e) => handleTeamsUrlChange(e.target.value)}
                                    />
                                    {urlExtractionStatus === 'success' && (
                                        <Typography.Text
                                            style={{ color: '#52c41a', fontSize: '12px', marginTop: '4px' }}
                                        >
                                            ✅ Tenant ID extracted successfully
                                        </Typography.Text>
                                    )}
                                    {urlExtractionStatus === 'error' && (
                                        <Typography.Text
                                            style={{ color: '#ff4d4f', fontSize: '12px', marginTop: '4px' }}
                                        >
                                            ❌ Could not find tenant ID in this URL
                                        </Typography.Text>
                                    )}
                                </SettingValueContainer>
                            </Form.Item>
                            <Form.Item label={<Typography.Text strong>Tenant ID</Typography.Text>}>
                                <Typography.Text type="secondary">
                                    Automatically extracted from your Teams URL.
                                </Typography.Text>
                                <SettingValueContainer>
                                    <StyledInput
                                        value={connection.tenant_id || ''}
                                        placeholder="Will be extracted from Teams URL above"
                                        data-testid="tenant-id-input"
                                        disabled
                                        style={{ backgroundColor: '#f5f5f5' }}
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                        </Form>
                        <Button
                            variant="outline"
                            onClick={startOAuthFlow}
                            data-testid="connect-to-teams-button"
                            disabled={!isFormValid}
                            style={{ display: 'block' }}
                        >
                            <PlatformLogo preview={false} src={teamsLogo} alt="teams-logo" />
                            {isConnected ? 'Re-connect to Teams' : 'Connect to Teams'}
                        </Button>
                    </FormColumn>
                </Content>
            </ContentContainer>
        </Page>
    );
};
