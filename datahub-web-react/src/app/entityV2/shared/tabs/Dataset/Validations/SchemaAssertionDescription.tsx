/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React, { useState } from 'react';

import { SchemaSummaryModal } from '@app/entityV2/shared/tabs/Dataset/Validations/SchemaSummaryModal';

import { SchemaAssertionCompatibility, SchemaAssertionInfo } from '@types';

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
