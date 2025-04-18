import { InfoCircleOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { SchemaBuilderTable } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/schema/SchemaBuilderTable';

import { SchemaAssertionField } from '@types';

const Tip = styled.div`
    margin-top: 12px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    justify-content: left;
    border: 1px solid ${ANTD_GRAY[5]};
    padding: 12px;
    background-color: ${ANTD_GRAY[3]};
    border-radius: 8px;
`;

const TipText = styled.div`
    && {
        word-wrap: break-word;
        white-space: break-spaces;
    }
`;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    color: ${ANTD_GRAY[7]};
    margin-right: 12px;
`;

type Props = {
    selected: Partial<SchemaAssertionField>[];
    onChange: (newFields: Partial<SchemaAssertionField>[]) => void;
    disabled?: boolean;
    options?: SchemaAssertionField[];
};

/**
 * Schema builder component for the assertion
 */
export const SchemaBuilder = ({ selected, onChange, disabled, options }: Props) => {
    return (
        <>
            <Typography.Title level={5}>Expected Columns</Typography.Title>
            <Typography.Paragraph type="secondary">
                Select the set of expected columns. These will be compared against the actual columns whenever changes
                are detected. This determines whether the assertion is passing or failing.
            </Typography.Paragraph>
            <SchemaBuilderTable selected={selected} onChange={onChange} disabled={disabled} options={options} />
            <Tip>
                <StyledInfoCircleOutlined />
                <TipText>
                    The schema collected during ingestion syncs will be used to evaluate this assertion. Be sure to run
                    ingestion syncs on a regular basis to keep your schemas up to date.
                </TipText>
            </Tip>
        </>
    );
};
