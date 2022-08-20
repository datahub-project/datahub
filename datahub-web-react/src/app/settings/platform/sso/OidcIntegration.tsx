import { InfoCircleOutlined } from '@ant-design/icons';
import { Button, Card, Divider, Form, Input, message, Switch, Typography, Row, Col, Alert, Collapse } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useGetSsoSettingsQuery, useUpdateSsoSettingsMutation } from '../../../../graphql/settings.generated';
import { OidcSettings } from '../../../../types.generated';
import { REDESIGN_COLORS } from '../../../entity/shared/constants';
import { Message } from '../../../shared/Message';
import { PlatformSsoIntegrationBreadcrumb } from '../PlatformSsoIntegrationBreadcrumb';

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

const SettingValueContainer = styled.div`
    margin-top: 12px;
`;

const StyledInput = styled(Input)`
    width: 240px;
`;

const InstructionsCard = styled(Card)`
    padding: 4px 4px 0px 4px;
    margin: 0px 16px 16px 0px;
    width: 40%;
`;

const InstructionsTitle = styled(Typography.Text)`
    padding: 8px;
`;

const DEFAULT_SETTINGS: OidcSettings = {
    enabled: false,
    clientId: '',
    clientSecret: '',
    discoveryUri: '',
};

export const OidcIntegration = () => {
    const [oidcSettings, setOidcSettings] = useState<OidcSettings>(DEFAULT_SETTINGS);
    const baseUrl = `${window.location.origin}`;

    // TODO: Determine the best way to avoid this duplicate query. (Or cache)
    const { data, loading, error, refetch } = useGetSsoSettingsQuery();
    const [updateSsoSettings] = useUpdateSsoSettingsMutation();

    useEffect(() => {
        if (data?.globalSettings?.ssoSettings?.oidcSettings) {
            setOidcSettings(data?.globalSettings?.ssoSettings?.oidcSettings);
        }
    }, [data, setOidcSettings]);

    const updateOidcSettings = () => {
        const { __typename: typename, ...parsedOidcSettings } = oidcSettings;
        console.log(typename);
        updateSsoSettings({
            variables: {
                input: {
                    baseUrl,
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

    return (
        <Page>
            <ContentContainer>
                {loading && <Message type="loading" content="Loading settings..." style={{ marginTop: '10%' }} />}
                {error && <Alert type="error" message={error?.message || `Failed to load integration settings`} />}
                <PlatformSsoIntegrationBreadcrumb name="OIDC" />
                <Typography.Title level={3}>OIDC</Typography.Title>
                <Typography.Text type="secondary">Configure SSO with your OIDC provider</Typography.Text>
                <Divider />
                <Row>
                    <Col xs={12} xl={12}>
                        <Form layout="vertical">
                            <Form.Item label={<Typography.Text strong>Enabled</Typography.Text>}>
                                <Typography.Text type="secondary">Turn the integration on or off.</Typography.Text>
                                <SettingValueContainer>
                                    <Switch
                                        checked={oidcSettings.enabled}
                                        onChange={(checked) => setOidcSettings({ ...oidcSettings, enabled: checked })}
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                            <Form.Item required label={<Typography.Text strong>Client ID</Typography.Text>}>
                                <Typography.Text type="secondary">
                                    Unique Client ID issued by the identity provider..
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
                            <Form.Item required label={<Typography.Text strong>Client Secret</Typography.Text>}>
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
                            <Form.Item required label={<Typography.Text strong>Discovery URI</Typography.Text>}>
                                <Typography.Text type="secondary">The IdP OIDC discovery url.</Typography.Text>
                                <SettingValueContainer>
                                    <StyledInput
                                        value={oidcSettings.discoveryUri || ''}
                                        placeholder="https://domain.idp.com/.well-known/openid-configuration"
                                        onChange={(e) =>
                                            setOidcSettings({
                                                ...oidcSettings,
                                                discoveryUri: e.target.value,
                                            })
                                        }
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                            <Collapse ghost style={{ marginLeft: -16, marginBottom: 10 }}>
                                <Collapse.Panel
                                    header={<Typography.Text type="secondary">Advanced</Typography.Text>}
                                    key="1"
                                >
                                    <Form.Item
                                        label={<Typography.Text strong>JIT Provisioning Enabled</Typography.Text>}
                                    >
                                        <Typography.Text type="secondary">
                                            Whether DataHub users should be provisioned on login if they do not exist.
                                            Defaults to <b> true</b>
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <Switch
                                                checked={
                                                    oidcSettings?.jitProvisioningEnabled
                                                        ? oidcSettings?.jitProvisioningEnabled
                                                        : true
                                                }
                                                onChange={(checked) =>
                                                    setOidcSettings({
                                                        ...oidcSettings,
                                                        jitProvisioningEnabled: checked,
                                                    })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>
                                    <Form.Item
                                        label={<Typography.Text strong>Pre-Provisioning Required</Typography.Text>}
                                    >
                                        <Typography.Text type="secondary">
                                            Whether the user should already exist in DataHub on login, failing login if
                                            they are not. Defaults to <b> false</b>
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <Switch
                                                checked={
                                                    oidcSettings.preProvisioningRequired
                                                        ? oidcSettings.preProvisioningRequired
                                                        : false
                                                }
                                                onChange={(checked) =>
                                                    setOidcSettings({
                                                        ...oidcSettings,
                                                        preProvisioningRequired: checked,
                                                    })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>
                                    <Form.Item label={<Typography.Text strong>Extract Groups Enabled</Typography.Text>}>
                                        <Typography.Text type="secondary">
                                            Whether groups should be extracted from a claim in the OIDC profile. Only
                                            applies if JIT provisioning is enabled. Groups will be created if they do
                                            not exist. Defaults to <b> false</b>
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <Switch
                                                checked={
                                                    oidcSettings.extractGroupsEnabled
                                                        ? oidcSettings.extractGroupsEnabled
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
                                    <Form.Item label={<Typography.Text strong>Scope</Typography.Text>}>
                                        <Typography.Text type="secondary">
                                            String representing the requested scope from the IdP. Defaults to{' '}
                                            <b> oidc email profile</b>.
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <StyledInput
                                                value={oidcSettings.scope || 'oidc email profile'}
                                                placeholder="oidc email profile"
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
                                            clientSecret) to the token endpoint: Defaults to. Defaults to{' '}
                                            <b> client_secret_basic</b>.
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
                                </Collapse.Panel>
                            </Collapse>
                        </Form>
                        <Button
                            disabled={
                                !oidcSettings.clientId || !oidcSettings.clientSecret || !oidcSettings.discoveryUri
                            }
                            style={{ marginBottom: 12 }}
                            onClick={() => updateOidcSettings()}
                        >
                            Update
                        </Button>
                    </Col>
                    <Col xs={12} xl={12}>
                        <InstructionsCard
                            title={
                                <>
                                    <InfoCircleOutlined style={{ color: REDESIGN_COLORS.BLUE }} />
                                    <InstructionsTitle strong>Prerequisites</InstructionsTitle>
                                </>
                            }
                        >
                            <Typography.Paragraph>
                                Enabling the SSO OIDC integration requires that you first register as a client with your
                                Identity Provider. Instructions are provided below for common OIDC Identity Providers.
                                For more detailed information, please read the official{' '}
                                <a
                                    target="_blank"
                                    rel="noreferrer"
                                    href="https://datahubproject.io/docs/authentication/guides/sso/configure-oidc-react"
                                >
                                    DataHub docs
                                </a>
                                .
                                <ul>
                                    <li>
                                        <a target="_blank" rel="noreferrer" href="https://www.okta.com/openid-connect/">
                                            Okta
                                        </a>
                                    </li>
                                    <li>
                                        <a
                                            target="_blank"
                                            rel="noreferrer"
                                            href="https://developers.google.com/identity/protocols/oauth2/openid-connect"
                                        >
                                            Google
                                        </a>
                                    </li>
                                    <li>
                                        <a
                                            target="_blank"
                                            rel="noreferrer"
                                            href="https://docs.microsoft.com/en-us/azure/active-directory/fundamentals/auth-oidc"
                                        >
                                            Azure AD
                                        </a>
                                    </li>
                                    <li>
                                        <a
                                            target="_blank"
                                            rel="noreferrer"
                                            href="https://www.keycloak.org/docs/latest/securing_apps/"
                                        >
                                            Keycloak
                                        </a>
                                    </li>
                                </ul>
                            </Typography.Paragraph>
                        </InstructionsCard>
                    </Col>
                </Row>
            </ContentContainer>
        </Page>
    );
};
