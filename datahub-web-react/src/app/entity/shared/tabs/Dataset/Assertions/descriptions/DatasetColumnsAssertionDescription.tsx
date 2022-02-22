import React from 'react';
import { DatasetColumnAssertion, StringMapEntry } from '../../../../../../../types.generated';

type Props = {
    assertion: DatasetColumnAssertion;
    parameters?: Array<StringMapEntry> | undefined;
};

export const DatasetColumnAssertionDescription = ({ assertion, parameters }: Props) => {
    console.log(assertion);
    console.log(parameters);
    return (
        <>
            Column <b>id</b> is unique.
        </>
    );
};
