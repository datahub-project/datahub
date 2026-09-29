import { Alert } from '@components';
import get from 'lodash/get';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

// Kept in sync with the CLI warning emitted at config validation.
const SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL =
    'https://docs.datahub.com/docs/quick-ingestion-guides/snowflake/migrate-to-key-pair-auth';

const AUTH_TYPE_FIELD_PATH = 'source.config.authentication_type';
const PASSWORD_FIELD_PATH = 'source.config.password';
const PRIVATE_KEY_FIELD_PATH = 'source.config.private_key';

/**
 * Determines whether a Snowflake recipe uses the deprecated username+password
 * authenticator. An explicit `authentication_type` wins; otherwise the auth type
 * is inferred from which credentials are present.
 *
 * Intentionally broader than the CLI's `is_using_password_auth()`, which requires
 * `DEFAULT_AUTHENTICATOR` *and* a non-empty password. The UI nags as soon as the
 * user picks "Username & Password" (the explicit field alone), so the banner
 * surfaces the guidance before a password is even entered. Do not "fix" this to
 * match the CLI predicate without considering that UX tradeoff.
 */
function isSnowflakePasswordAuth(recipe: any): boolean {
    const authType = get(recipe, AUTH_TYPE_FIELD_PATH);
    if (authType) {
        return authType === 'DEFAULT_AUTHENTICATOR';
    }
    const hasPassword = !!get(recipe, PASSWORD_FIELD_PATH);
    const hasPrivateKey = !!get(recipe, PRIVATE_KEY_FIELD_PATH);
    return hasPassword && !hasPrivateKey;
}

interface Props {
    /** Parsed recipe object. When null/undefined the banner is hidden. */
    recipe: any;
}

/**
 * Warns that the Snowflake recipe uses deprecated username+password auth and
 * links to the key-pair migration guide. Shared across the legacy and ingestV2
 * source builders.
 */
export const SnowflakePasswordAuthDeprecationWarning = ({ recipe }: Props) => {
    const { t } = useTranslation('ingestion.sourceBuilder');

    if (!recipe || !isSnowflakePasswordAuth(recipe)) {
        return null;
    }

    // Link text is injected at runtime by <Trans> from the i18n message string, so the
    // <a> element is intentionally childless here.
    /* eslint-disable jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label */
    const anchor = <a href={SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL} target="_blank" rel="noopener noreferrer" />;
    /* eslint-enable jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label */

    return (
        <Alert
            style={{ marginBottom: '10px' }}
            variant="warning"
            data-testid="snowflake-password-auth-deprecation-warning"
            title={<Trans t={t} i18nKey="snowflake.passwordAuthDeprecation.message" components={{ anchor }} />}
        />
    );
};
