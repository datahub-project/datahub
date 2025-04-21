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
