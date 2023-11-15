import { Typography, Button } from 'antd';
import React, { useState } from 'react';
import { SchemaAssertionInfo } from '../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../constants';
import { SchemaSummaryModal } from './SchemaSummaryModal';

type Props = {
    assertionInfo: SchemaAssertionInfo;
};

/**
 * A human-readable description of a Schema Assertion.
 */
export const SchemaAssertionDescription = ({ assertionInfo }: Props) => {
    const [showSchemaSummary, setShowSchemaSummary] = useState(false);
    return (
        <div>
            <Typography.Text>
                Dataset columns match expected schema
                <Button
                    type="text"
                    style={{ color: REDESIGN_COLORS.BLUE, marginLeft: 8 }}
                    onClick={() => setShowSchemaSummary(true)}
                >
                    view schema
                </Button>
            </Typography.Text>
            {showSchemaSummary && (
                <SchemaSummaryModal schema={assertionInfo.schema} onClose={() => setShowSchemaSummary(false)} />
            )}
        </div>
    );
};
