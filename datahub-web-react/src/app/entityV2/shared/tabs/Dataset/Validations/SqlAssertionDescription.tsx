import { OverflowText } from '@components';
import React from 'react';

import { AssertionInfo } from '@types';

type Props = {
    assertionInfo: AssertionInfo;
    ellipsis?: boolean;
};

/**
 * A human-readable description of a SQL Assertion.
 */
export const SqlAssertionDescription = ({ assertionInfo, ellipsis }: Props) => {
    const { description } = assertionInfo;

    return ellipsis ? <OverflowText text={description || ''} /> : <span>{description}</span>;
};
