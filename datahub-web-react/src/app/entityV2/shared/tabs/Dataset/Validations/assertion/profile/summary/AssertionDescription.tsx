import React from 'react';

import {
    Assertion,
    AssertionInfo,
    AssertionType,
    DatasetAssertionInfo,
    FieldAssertionInfo,
    FreshnessAssertionInfo,
    SchemaAssertionInfo,
    VolumeAssertionInfo,
} from '../../../../../../../../../types.generated';
import { DatasetAssertionDescription } from '../../../DatasetAssertionDescription';
import { FreshnessAssertionDescription } from '../../../FreshnessAssertionDescription';
import { VolumeAssertionDescription } from '../../../VolumeAssertionDescription';
import { SqlAssertionDescription } from '../../../SqlAssertionDescription';
import { FieldAssertionDescription } from '../../../FieldAssertionDescription';
import { SchemaAssertionDescription } from '../../../SchemaAssertionDescription';

type Props = {
    assertion: Assertion;
};

// Component useful for rendering descriptions of assertions.
export const AssertionDescription = ({ assertion }: Props) => {
    if (!assertion.info) {
        return <>No description found</>;
    }
    if (assertion.info?.description) {
        return <>{assertion.info.description}</>;
    }
    const assertionInfo = assertion.info as AssertionInfo;
    const assertionType = assertionInfo.type;
    return (
        <>
            {assertionType === AssertionType.Dataset && (
                <DatasetAssertionDescription assertionInfo={assertionInfo.datasetAssertion as DatasetAssertionInfo} />
            )}
            {assertionType === AssertionType.Freshness && (
                <FreshnessAssertionDescription
                    assertionInfo={assertionInfo.freshnessAssertion as FreshnessAssertionInfo}
                />
            )}
            {assertionType === AssertionType.Volume && (
                <VolumeAssertionDescription assertionInfo={assertionInfo.volumeAssertion as VolumeAssertionInfo} />
            )}
            {assertionType === AssertionType.Sql && <SqlAssertionDescription assertionInfo={assertionInfo} />}
            {assertionType === AssertionType.Field && (
                <FieldAssertionDescription assertionInfo={assertionInfo.fieldAssertion as FieldAssertionInfo} />
            )}
            {assertionType === AssertionType.DataSchema && (
                <SchemaAssertionDescription assertionInfo={assertionInfo.schemaAssertion as SchemaAssertionInfo} />
            )}
        </>
    );
};
