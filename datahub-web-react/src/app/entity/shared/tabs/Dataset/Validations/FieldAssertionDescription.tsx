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

import {
    getFieldDescription,
    getFieldOperatorDescription,
    getFieldParametersDescription,
    getFieldTransformDescription,
} from '@app/entity/shared/tabs/Dataset/Validations/fieldDescriptionUtils';

import { FieldAssertionInfo } from '@types';

type Props = {
    assertionInfo: FieldAssertionInfo;
};

/**
 * A human-readable description of a Field Assertion.
 */
export const FieldAssertionDescription = ({ assertionInfo }: Props) => {
    const field = getFieldDescription(assertionInfo);
    const operator = getFieldOperatorDescription(assertionInfo);
    const transform = getFieldTransformDescription(assertionInfo);
    const parameters = getFieldParametersDescription(assertionInfo);

    return (
        <Typography.Text>
            {transform}
            {transform ? ' of ' : ''}
            <Typography.Text code>{field}</Typography.Text> {operator} {parameters}
        </Typography.Text>
    );
};
