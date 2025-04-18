import React from 'react';
import styled from 'styled-components';
import { Form, Typography } from 'antd';
import {
    AssertionStdOperator,
    AssertionStdParameters,
    VolumeAssertionInfo,
} from '../../../../../../../../../../types.generated';
import { getParameterBuilderTitle } from './utils';
import { VolumeRowCountTotalBuilder } from './VolumeRowCountTotalBuilder';
import { VolumeRowCountChangeBuilder } from './VolumeRowCountChangeBuilder';
import { getIsRowCountChange, getPropertyFromVolumeType } from '../../../../utils';

const FormDiv = styled.div`
    margin: 16px 0 24px;
    background: white;
`;

type Props = {
    volumeInfo: VolumeAssertionInfo;
    value: AssertionStdParameters;
    onChange: (newParams: AssertionStdParameters) => void;
    updateVolumeAssertion: (newParams: Partial<VolumeAssertionInfo>) => void;
    disabled?: boolean;
};

export const VolumeParametersBuilder = (props: Props) => {
    const form = Form.useFormInstance();
    const { volumeInfo } = props;
    const selectedType = volumeInfo?.type;
    const isRowCountChange = selectedType ? getIsRowCountChange(selectedType) : null;
    const propertyName = selectedType ? getPropertyFromVolumeType(selectedType) : '';
    const operator = selectedType ? (volumeInfo?.[propertyName]?.operator as AssertionStdOperator) : null;
    const title = selectedType && operator ? getParameterBuilderTitle(selectedType, operator) : '';
    const showParameters = selectedType && operator && form.getFieldValue('volume-type');

    return showParameters ? (
        <FormDiv>
            <Typography.Title level={5}>{title}</Typography.Title>
            {isRowCountChange ? <VolumeRowCountChangeBuilder {...props} /> : <VolumeRowCountTotalBuilder {...props} />}
        </FormDiv>
    ) : null;
};
