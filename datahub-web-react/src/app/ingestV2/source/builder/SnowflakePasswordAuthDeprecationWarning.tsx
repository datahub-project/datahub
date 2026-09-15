import { Alert } from '@components';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

import { getSnowflakeAuthTypeFromRecipe } from '@app/ingestV2/source/builder/RecipeForm/snowflake';

// Kept in sync with the CLI warning (snowflake_auth_deprecation.py).
const SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL =
    'https://docs.datahub.com/docs/quick-ingestion-guides/snowflake/migrate-to-key-pair-auth';

interface Props {
    /** Parsed recipe object. When null/undefined the banner is hidden. */
    recipe: any;
}

/**
 * Warns that the Snowflake recipe uses deprecated username+password auth and
 * links to the key-pair migration guide.
 */
export const SnowflakePasswordAuthDeprecationWarning = ({ recipe }: Props) => {
    const { t } = useTranslation('ingestion.sourceBuilder');

    if (!recipe || getSnowflakeAuthTypeFromRecipe(recipe) !== 'DEFAULT_AUTHENTICATOR') {
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
