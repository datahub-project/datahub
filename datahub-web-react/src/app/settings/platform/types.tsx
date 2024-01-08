import React from 'react';
import oidcLogo from '../../../images/oidclogo.png';
// import { NotificationScenarioType, NotificationSettingValue } from '../../../types.generated';
// import { SlackIntegration } from './slack/SlackIntegration';
import { OidcIntegration } from './sso/OidcIntegration';

/**
 * SSO Integrations
 */

const OIDC_INTEGRATION = {
    id: 'oidc',
    name: 'OIDC',
    img: oidcLogo,
    description: 'Integrate DataHub with your OIDC SSO provider ',
    content: <OidcIntegration />,
};

export const SUPPORTED_SSO_INTEGRATIONS = [OIDC_INTEGRATION];

