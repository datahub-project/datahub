import React from 'react';
import { Assertion, AssertionType } from '../../../../../../../../../types.generated';
import { SqlAssertionDetails } from './SqlAssertionDetails';
import { FreshnessAssertionDetails } from './FreshnessAssertionDetails';
import { VolumeAssertionDetails } from './VolumeAssertionDetails';
import { FieldAssertionDetails } from './FieldAssertionDetails';

type Props = {
    assertion: Assertion;
};

export const AssertionDetails = (props: Props) => {
    switch (props.assertion.info?.type) {
        case AssertionType.Freshness:
            return <FreshnessAssertionDetails {...props} />;
        case AssertionType.Volume:
            return <VolumeAssertionDetails {...props} />;
        case AssertionType.Field:
            return <FieldAssertionDetails {...props} />;
        case AssertionType.Sql:
            return <SqlAssertionDetails {...props} />;
        default:
            return null;
    }
};
