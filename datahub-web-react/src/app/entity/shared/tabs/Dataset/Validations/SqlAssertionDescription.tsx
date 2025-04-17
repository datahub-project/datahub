import { Typography } from 'antd';
import React from 'react';

import { AssertionInfo } from '@types';

type Props = {
    assertionInfo: AssertionInfo;
};

/**
 * A human-readable description of a SQL Assertion.
 */
export const SqlAssertionDescription = ({ assertionInfo }: Props) => {
    const { description } = assertionInfo;

    return <Typography.Text>{description}</Typography.Text>;
};
