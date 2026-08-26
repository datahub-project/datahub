import { Typography } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { SchemaSummaryModal } from '@app/entityV2/shared/tabs/Dataset/Validations/SchemaSummaryModal';

import { SchemaAssertionCompatibility, SchemaAssertionInfo } from '@types';

type Props = {
    assertionInfo: SchemaAssertionInfo;
};

/**
 * A human-readable description of a Schema Assertion.
 */
export const SchemaAssertionDescription = ({ assertionInfo }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const [showSchemaSummary, setShowSchemaSummary] = useState(false);
    const { compatibility } = assertionInfo;
    const isExactMatch = compatibility === SchemaAssertionCompatibility.ExactMatch;
    const expectedColumnCount = assertionInfo?.fields?.length || 0;
    return (
        <div>
            <Typography.Text>
                {t(isExactMatch ? 'schemaDescription.exactMatch' : 'schemaDescription.include', {
                    count: expectedColumnCount,
                })}
            </Typography.Text>
            {showSchemaSummary && !!assertionInfo.schema && (
                <SchemaSummaryModal schema={assertionInfo.schema} onClose={() => setShowSchemaSummary(false)} />
            )}
        </div>
    );
};
