import { Maybe, OidcSettings, SsoSettings } from '@types';

export const checkIsOidcEnabled = (oidcSettings?: Maybe<OidcSettings>) => {
    return oidcSettings?.enabled;
};

export const checkIsSsoEnabled = (ssoSettings: SsoSettings) => {
    return checkIsOidcEnabled(ssoSettings.oidcSettings);
};

export const checkIsOidcConfigured = (oidcSettings?: Maybe<OidcSettings>) => {
    return oidcSettings?.clientId && oidcSettings?.clientSecret && oidcSettings?.discoveryUri;
};

export const checkIsSsoConfigured = (ssoSettings?: Maybe<SsoSettings>) => {
    return checkIsOidcConfigured(ssoSettings?.oidcSettings);
};
