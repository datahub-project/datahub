import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import useFormInstance from 'antd/lib/form/hooks/useFormInstance';
import {
    AssertionStdOperator,
    AssertionStdParameters,
    VolumeAssertionInfo,
} from '../../../../../../../../../../types.generated';
import { getIsRowCountChange, getParameterBuilderTitle, getPropertyFromVolumeType } from './utils';
import { VolumeRowCountTotalBuilder } from './VolumeRowCountTotalBuilder';
import { VolumeRowCountChangeBuilder } from './VolumeRowCountChangeBuilder';

const Form = styled.div`
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
    const form = useFormInstance();
    const { volumeInfo } = props;
    const selectedType = volumeInfo?.type;
    const isRowCountChange = selectedType ? getIsRowCountChange(selectedType) : null;
    const propertyName = selectedType ? getPropertyFromVolumeType(selectedType) : '';
    const operator = selectedType ? (volumeInfo?.[propertyName]?.operator as AssertionStdOperator) : null;
    const title = selectedType && operator ? getParameterBuilderTitle(selectedType, operator) : '';
    const showParameters = selectedType && operator && form.getFieldValue('volume-type');

    return showParameters ? (
        <Form>
            <Typography.Title level={5}>{title}</Typography.Title>
            {isRowCountChange ? <VolumeRowCountChangeBuilder {...props} /> : <VolumeRowCountTotalBuilder {...props} />}
        </Form>
    ) : null;
};
