import React, { useState } from 'react';
import { Typography } from 'antd';
import { SchemaSummaryModal } from './SchemaSummaryModal';
import { SchemaAssertionInfo, SchemaAssertionCompatibility } from '../../../../../../types.generated';

type Props = {
    assertionInfo: SchemaAssertionInfo;
};

/**
 * A human-readable description of a Schema Assertion.
 */
export const SchemaAssertionDescription = ({ assertionInfo }: Props) => {
    const [showSchemaSummary, setShowSchemaSummary] = useState(false);
    const { compatibility } = assertionInfo;
    const matchText = compatibility === SchemaAssertionCompatibility.ExactMatch ? 'exactly match' : 'include';
    const expectedColumnCount = assertionInfo?.fields?.length || 0;
    return (
        <div>
            <Typography.Text>
                Actual table columns {matchText} {expectedColumnCount} expected columns
            </Typography.Text>
            {showSchemaSummary && !!assertionInfo.schema && (
                <SchemaSummaryModal schema={assertionInfo.schema} onClose={() => setShowSchemaSummary(false)} />
            )}
        </div>
    );
};
