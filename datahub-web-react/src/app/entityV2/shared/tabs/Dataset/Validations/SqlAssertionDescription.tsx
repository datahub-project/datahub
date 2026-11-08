import { Typography } from 'antd';
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

    return <Typography.Text ellipsis={ellipsis ? { tooltip: true } : undefined}>{description}</Typography.Text>;
};
