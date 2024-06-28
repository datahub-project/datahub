import React from 'react';
import { Typography } from 'antd';
import { FieldAssertionInfo } from '../../../../../../types.generated';
import {
    getFieldDescription,
    getFieldOperatorDescription,
    getFieldParametersDescription,
    getFieldTransformDescription,
} from './fieldDescriptionUtils';

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
