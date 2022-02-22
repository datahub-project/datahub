import React from 'react';
import { DatasetRowsAssertion, StringMapEntry } from '../../../../../../../types.generated';

type Props = {
    assertion: DatasetRowsAssertion;
    parameters?: Array<StringMapEntry> | undefined;
};

export const DatasetRowsAssertionDescription = ({ assertion, parameters }: Props) => {
    console.log(assertion);
    console.log(parameters);
    return <>Hi</>;
};
