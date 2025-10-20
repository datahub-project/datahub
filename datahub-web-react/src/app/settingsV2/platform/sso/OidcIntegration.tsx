import { green, yellow } from '@ant-design/colors';
import { InfoCircleOutlined, WarningOutlined } from '@ant-design/icons';
import { Alert, Col, Collapse, Divider, Form, Input, Radio, Row, Space, Switch, Typography, message } from 'antd';
import _ from 'lodash';
import React, { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { PlatformSsoIntegrationBreadcrumb } from '@app/settingsV2/platform/PlatformSsoIntegrationBreadcrumb';
import { OidcIntegrationHint } from '@app/settingsV2/platform/sso/OidcIntegrationHint';
import { checkIsOidcConfigured, checkIsOidcEnabled } from '@app/settingsV2/platform/sso/utils';
import { Message } from '@app/shared/Message';
import { Button } from '@src/alchemy-components';
import { getColor } from '@src/alchemy-components/theme/utils';
import analytics, { EventType, ObfuscatedOidcSettings } from '@src/app/analytics';
import { getRuntimeBasePath } from '@utils/runtimeBasePath';

import { useGetSsoSettingsQuery, useUpdateSsoSettingsMutation } from '@graphql/settings.generated';
import { OidcSettings } from '@types';

const Page = styled.div`
    width: 100%;
    display: flex;
    justify-content: center;
    overflow: auto;
`;

const ContentContainer = styled.div`
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
    width: 100%;
`;

const SettingValueContainer = styled.div`
    margin-top: 12px;
`;

const StyledInput = styled(Input)`
    width: 240px;
`;

const ExampleDiscoveryUrl = styled(Typography.Text)`
    background-color: ${getColor('gray', 1500)};
    padding: 2px 6px;
    margin-top: 4px;
    color: ${getColor('gray', 400)};
    display: inline-block;
`;

const ConfigSuccessfulBanner = styled.div`
    background-color: ${green[0]};
    color: ${green[8]};
    border-radius: 8px;
    border: 1px solid ${green[6]};
    padding: 8px 16px;
    margin: 18px 0 25px;
    font-size: 14px;
    display: flex;
    align-items: center;
`;

const ConfigDisabledBanner = styled.div`
    background-color: ${yellow[0]};
    color: ${yellow[8]};
    border-radius: 8px;
    border: 1px solid ${yellow[6]};
    padding: 8px 16px;
    margin: 18px 0 25px;
    font-size: 14px;
    display: flex;
    align-items: center;
`;

const ToggleEnableButton = styled.div`
    text-decoration: underline;
    font-weight: 600;
    cursor: pointer;
    color: black;
`;

const DEFAULT_SETTINGS: OidcSettings = {
    enabled: false,
    clientId: '',
    clientSecret: '',
    discoveryUri: '',
};

const BASE_URL = `${window.location.origin}${getRuntimeBasePath()}`;

const isConfigAbsent = (field: any): boolean => {
    if (field === null || field === undefined) {
        return true;
    }
    return false;
};

// NOTE: Do not put anything sensitive here
const WHITELISTED_FIELDS_FOR_ANALYTICS: (keyof OidcSettings)[] = [
    'userNameClaim',
    'userNameClaimRegex',
    'groupsClaim',
    'scope',
    'clientAuthenticationMethod',
    'preferredJwsAlgorithm',
];

const oidcSettingsToObfuscatedAnalyticsReport = (settings: OidcSettings): ObfuscatedOidcSettings => {
    const map: ObfuscatedOidcSettings = {};
    return Object.entries(settings)
        .map((entry) =>
            entry[1] === WHITELISTED_FIELDS_FOR_ANALYTICS.includes(entry[0] as keyof OidcSettings)
                ? String(entry[1])
                : !!entry[1],
        )
        .reduce((o, entry) => ({ ...o, [entry[0]]: entry[1] }), map);
};

export const OidcIntegration = () => {
    const { data, loading, error, refetch } = useGetSsoSettingsQuery();

    const persistedOidcSettings = data?.globalSettings?.ssoSettings?.oidcSettings;

    const [updateSsoSettings] = useUpdateSsoSettingsMutation();

    const [oidcSettings, setOidcSettings] = useState<OidcSettings>(DEFAULT_SETTINGS);

    // We will customize on save behavior and tips rendering depending on if SSO is currently enabled
    const isIntegrationEnabled = checkIsOidcEnabled(oidcSettings);
    // This will determine what tips we show in the banner, and whether we should automatically enable on update
    const isIntegrationConfigured = checkIsOidcConfigured(persistedOidcSettings);
    // for analytics
    const isAdvancedSectionExpandedRef = useRef(false);

    useEffect(() => {
        if (persistedOidcSettings) {
            setOidcSettings(persistedOidcSettings);
        }
    }, [persistedOidcSettings, setOidcSettings]);

    const updateOidcSettings = (settings: OidcSettings) => {
        const parsedOidcSettings = _.omit(settings, '__typename');
        updateSsoSettings({
            variables: {
                input: {
                    baseUrl: BASE_URL,
                    oidcSettings: parsedOidcSettings,
                },
            },
        })
            .then(() => {
                message.success({ content: 'Updated OIDC SSO Settings!', duration: 2 });
                refetch?.();
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to update settings: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const toggleSSOEnabled = () => {
        const newSettings: OidcSettings = { ...oidcSettings, enabled: !oidcSettings.enabled };
        setOidcSettings(newSettings);
        updateOidcSettings(newSettings);
        analytics.event({
            type: EventType.SSOConfigurationEvent,
            action: oidcSettings.enabled ? 'disable-sso' : 'enable-sso',
        });
    };

    const saveOidcSettings = () => {
        const settingsToSave: OidcSettings = isIntegrationConfigured
            ? oidcSettings
            : { ...oidcSettings, enabled: true };

        analytics.event({
            type: EventType.SSOConfigurationEvent,
            action: isIntegrationConfigured ? 'initialize-sso' : 'update-sso',
            newSettings: oidcSettingsToObfuscatedAnalyticsReport(settingsToSave),
            oldSettings: persistedOidcSettings
                ? oidcSettingsToObfuscatedAnalyticsReport(persistedOidcSettings)
                : undefined,
            isAdvancedVisible: isAdvancedSectionExpandedRef.current,
        });
        updateOidcSettings(settingsToSave);
    };

    const configuredBanner = isIntegrationEnabled ? (
        <ConfigSuccessfulBanner>
            <InfoCircleOutlined style={{ marginRight: 8 }} />
            <span>The SSO integration is ready! Sign out and log back in with SSO to test it out.</span>
            <div style={{ flexGrow: 1 }} />
            <ToggleEnableButton onClick={toggleSSOEnabled}>Disable SSO</ToggleEnableButton>
        </ConfigSuccessfulBanner>
    ) : (
        <ConfigDisabledBanner>
            <WarningOutlined style={{ marginRight: 8 }} />
            <span>The SSO integration is disabled.</span>
            <div style={{ flexGrow: 1 }} />
            <ToggleEnableButton onClick={toggleSSOEnabled}>Enable</ToggleEnableButton>
        </ConfigDisabledBanner>
    );
    return (
        <Page>
            <ContentContainer>
                {loading && <Message type="loading" content="Loading settings..." style={{ marginTop: '10%' }} />}
                {error && <Alert type="error" message={error?.message || `Failed to load integration settings`} />}
                <PlatformSsoIntegrationBreadcrumb name="OIDC" />
                <Typography.Title level={3}>OIDC</Typography.Title>
                <Typography.Text type="secondary">Configure SSO with your OIDC provider</Typography.Text>
                <Divider />
                {isIntegrationConfigured
                    ? configuredBanner
                    : !loading && <OidcIntegrationHint visible={!isIntegrationConfigured} />}
                <Row>
                    <Col xl={12} xs={12}>
                        <Form layout="vertical">
                            <Form.Item
                                required
                                requiredMark="optional"
                                label={<Typography.Text strong>Client ID</Typography.Text>}
                            >
                                <Typography.Text type="secondary">
                                    Unique Client ID issued by the identity provider.
                                </Typography.Text>
                                <SettingValueContainer>
                                    <StyledInput
                                        value={oidcSettings.clientId || ''}
                                        placeholder="a1b2c3d4e5f6g7h8"
                                        onChange={(e) =>
                                            setOidcSettings({
                                                ...oidcSettings,
                                                clientId: e.target.value,
                                            })
                                        }
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                            <Form.Item
                                required
                                requiredMark="optional"
                                label={<Typography.Text strong>Client Secret</Typography.Text>}
                            >
                                <Typography.Text type="secondary">
                                    Unique Client Secret issued by the identity provider. This will be <b> encrypted</b>{' '}
                                    and stored securely in DataHub.
                                </Typography.Text>
                                <SettingValueContainer>
                                    <StyledInput
                                        type="password"
                                        value={oidcSettings.clientSecret || ''}
                                        placeholder="secret"
                                        onChange={(e) =>
                                            setOidcSettings({
                                                ...oidcSettings,
                                                clientSecret: e.target.value,
                                            })
                                        }
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                            <Form.Item
                                required
                                requiredMark="optional"
                                label={<Typography.Text strong>Discovery URI</Typography.Text>}
                            >
                                <Typography.Text type="secondary">
                                    The IdP OIDC discovery url. It will end with
                                </Typography.Text>
                                <ExampleDiscoveryUrl>/.well-known/openid-configuration</ExampleDiscoveryUrl>.
                                <SettingValueContainer>
                                    <StyledInput
                                        value={oidcSettings.discoveryUri || ''}
                                        placeholder="https://<idp-base-path>/.well-known/openid-configuration"
                                        onChange={(e) =>
                                            setOidcSettings({
                                                ...oidcSettings,
                                                discoveryUri: e.target.value,
                                            })
                                        }
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                            <Form.Item label={<Typography.Text strong>User Provisioning Strategy</Typography.Text>}>
                                <Radio.Group
                                    onChange={(e) => {
                                        const isJIT = e.target.value === 1;
                                        setOidcSettings({
                                            ...oidcSettings,
                                            jitProvisioningEnabled: isJIT,
                                            preProvisioningRequired: !isJIT,
                                            extractGroupsEnabled: oidcSettings.extractGroupsEnabled && isJIT,
                                        });
                                    }}
                                    value={oidcSettings.jitProvisioningEnabled ? 1 : 2}
                                >
                                    <Space direction="vertical">
                                        <Radio value={1}>
                                            <Typography.Text>
                                                Just-in-Time (JIT) Provisioning <i>(Default)</i>
                                            </Typography.Text>
                                            <br />
                                            <Typography.Text type="secondary">
                                                Automatically creates a DataHub User on login if one does not exist.
                                            </Typography.Text>
                                        </Radio>
                                        <Radio value={2}>
                                            <Typography.Text>Pre-Provisioning DataHub Users</Typography.Text>
                                            <br />
                                            <Typography.Text type="secondary">
                                                Only allows login for pre-provisioned DataHub Users.
                                                <br />
                                                <i>
                                                    Requires configuring <Link to="/ingestion">SSO ingestion</Link> to
                                                    create DataHub Users.
                                                </i>
                                            </Typography.Text>
                                        </Radio>
                                    </Space>
                                </Radio.Group>
                            </Form.Item>
                            <Form.Item label={<Typography.Text strong>Extract Groups</Typography.Text>}>
                                <Typography.Text type="secondary">
                                    Extracts Group membership from the{' '}
                                    <b style={{ backgroundColor: '#f9f9f9' }}>groups</b> claim in the OIDC profile by
                                    default. Automatically creates a DataHub Group if one does not exist.
                                    <br />
                                    <i>Requires JIT Provisioning.</i>
                                </Typography.Text>
                                <SettingValueContainer>
                                    <Switch
                                        disabled={!oidcSettings.jitProvisioningEnabled}
                                        checked={
                                            !isConfigAbsent(oidcSettings.extractGroupsEnabled)
                                                ? (oidcSettings.extractGroupsEnabled as boolean)
                                                : false
                                        }
                                        onChange={(checked) =>
                                            setOidcSettings({
                                                ...oidcSettings,
                                                extractGroupsEnabled: checked,
                                            })
                                        }
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                            <Collapse
                                ghost
                                style={{ marginLeft: -16, marginBottom: 10 }}
                                onChange={(keys) => {
                                    isAdvancedSectionExpandedRef.current = !!keys.length;
                                    if (isAdvancedSectionExpandedRef.current) {
                                        analytics.event({
                                            type: EventType.SSOConfigurationEvent,
                                            action: 'expand-advanced',
                                        });
                                    }
                                }}
                            >
                                <Collapse.Panel
                                    header={<Typography.Text type="secondary">Advanced</Typography.Text>}
                                    key="1"
                                >
                                    <Form.Item label={<Typography.Text strong>User Name Claim</Typography.Text>}>
                                        <Typography.Text type="secondary">
                                            The attribute / claim used to derive the DataHub username. Defaults to
                                            <b> preferred_username</b>.
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <StyledInput
                                                value={oidcSettings.userNameClaim || 'preferred_username'}
                                                placeholder="preferred_username"
                                                onChange={(e) =>
                                                    setOidcSettings({
                                                        ...oidcSettings,
                                                        userNameClaim: e.target.value,
                                                    })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>

                                    <Form.Item label={<Typography.Text strong>User Name Claim Regex</Typography.Text>}>
                                        <Typography.Text type="secondary">
                                            The regex used to parse the DataHub username from the user name claim.
                                            Defaults to (.*) (all).
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <StyledInput
                                                value={oidcSettings?.userNameClaimRegex || '.*'}
                                                placeholder="User Name Claim Regex"
                                                onChange={(e) =>
                                                    setOidcSettings({
                                                        ...oidcSettings,
                                                        userNameClaimRegex: e.target.value,
                                                    })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>

                                    <Form.Item label={<Typography.Text strong>Groups Claim</Typography.Text>}>
                                        <Typography.Text type="secondary">
                                            The OIDC claim to extract groups information from. Defaults to{' '}
                                            <b> groups</b>.
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <StyledInput
                                                value={oidcSettings?.groupsClaim || 'groups'}
                                                placeholder="groups"
                                                onChange={(e) =>
                                                    setOidcSettings({
                                                        ...oidcSettings,
                                                        groupsClaim: e.target.value,
                                                    })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>

                                    <Form.Item label={<Typography.Text strong>Scope</Typography.Text>}>
                                        <Typography.Text type="secondary">
                                            String representing the requested scope from the IdP. Defaults to{' '}
                                            <b> openid email profile</b>.
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <StyledInput
                                                value={oidcSettings.scope || 'openid email profile'}
                                                placeholder="openid email profile"
                                                onChange={(e) =>
                                                    setOidcSettings({
                                                        ...oidcSettings,
                                                        scope: e.target.value,
                                                    })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>
                                    <Form.Item
                                        label={<Typography.Text strong>Client Authentication Method</Typography.Text>}
                                    >
                                        <Typography.Text type="secondary">
                                            Which authentication method to use to pass credentials (clientId and
                                            clientSecret) to the token endpoint. Defaults to <b> client_secret_basic</b>
                                            .
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <StyledInput
                                                value={oidcSettings.clientAuthenticationMethod || 'client_secret_basic'}
                                                placeholder="client_secret_basic"
                                                onChange={(e) =>
                                                    setOidcSettings({
                                                        ...oidcSettings,
                                                        clientAuthenticationMethod: e.target.value,
                                                    })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>
                                    <Form.Item
                                        label={<Typography.Text strong>Preferred Jws Algorithm</Typography.Text>}
                                    >
                                        <Typography.Text type="secondary">Which jws algorithm to use</Typography.Text>
                                        <SettingValueContainer>
                                            <StyledInput
                                                value={oidcSettings?.preferredJwsAlgorithm || ''}
                                                placeholder="Preferred Jws Algorithm"
                                                onChange={(e) =>
                                                    setOidcSettings({
                                                        ...oidcSettings,
                                                        preferredJwsAlgorithm: e.target.value,
                                                    })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>
                                </Collapse.Panel>
                            </Collapse>
                        </Form>
                        <Button style={{ marginBottom: 20 }} onClick={saveOidcSettings}>
                            {isIntegrationConfigured ? 'Update' : 'Connect'}
                        </Button>
                    </Col>
                </Row>
            </ContentContainer>
        </Page>
    );
};
