import { Alert } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

import { getSnowflakeAuthTypeFromRecipe } from '@app/ingestV2/source/builder/RecipeForm/snowflake';

// Stable URL for the customer-facing migration guide. Kept in sync with the
// CLI warning (snowflake_auth_deprecation.py) and the structured report.
const SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL =
    'https://docs.datahub.com/docs/quick-ingestion-guides/snowflake/migrate-to-key-pair-auth';

interface Props {
    /** Parsed recipe JSON (source.config.*). When null/undefined the banner is hidden. */
    recipe: any;
}

/**
 * Warns the user that their Snowflake recipe is using deprecated username+password
 * auth (DEFAULT_AUTHENTICATOR) and links to the key-pair migration guide.
 *
 * Detection reuses getSnowflakeAuthTypeFromRecipe() so it stays consistent with the
 * form's own auth-type inference (including the CAT-1921 edge case where
 * authentication_type is omitted but a password is present).
 */
export const SnowflakePasswordAuthDeprecationWarning = ({ recipe }: Props) => {
    const { t } = useTranslation('ingestion.sourceBuilder');

    if (!recipe || getSnowflakeAuthTypeFromRecipe(recipe) !== 'DEFAULT_AUTHENTICATOR') {
        return null;
    }

    return (
        <Alert
            style={{ marginBottom: '10px' }}
            type="warning"
            banner
            message={
                <Trans
                    t={t}
                    i18nKey="snowflake.passwordAuthDeprecation.message"
                    components={{
                        anchor: (
                            <a href={SNOWFLAKE_PASSWORD_AUTH_DEPRECATION_URL} target="_blank" rel="noopener noreferrer">
                                {t('snowflake.passwordAuthDeprecation.linkText')}
                            </a>
                        ),
                    }}
                />
            }
        />
    );
};
