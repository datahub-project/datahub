import React from 'react';
import { Assertion, AssertionType } from '../../../../../../../../types.generated';
import { SqlAssertionDetails } from './SqlAssertionDetails';

type Props = {
    assertion: Assertion;
};

export const AssertionDetails = ({ assertion }: Props) => {
    switch (assertion.info?.type) {
        case AssertionType.Sql:
            return <SqlAssertionDetails assertion={assertion} />;
        default:
            return null;
    }
};
