import React from 'react';
import { DatasetSchemaAssertion, StringMapEntry } from '../../../../../../../types.generated';

type Props = {
    assertion: DatasetSchemaAssertion;
    parameters?: Array<StringMapEntry> | undefined;
};

export const DatasetSchemaAssertionDescription = ({ assertion, parameters }: Props) => {
    console.log(assertion);
    console.log(parameters);
    return <>Hi</>;
};
