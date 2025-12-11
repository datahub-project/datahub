/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
