import { Form, Radio, RadioChangeEvent, Space } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FieldLabel, StyledRadioGroup } from '../styledComponents';
import GlossaryTermsSelector from './GlossaryTermsSelector';
import CardinalityField from './CardinalityField';

const AllowedTermsWrapper = styled.div`
    margin-bottom: 24px;
`;

export default function GlossaryTermsQuestion() {
    const form = Form.useFormInstance();
    const allowedTerms =
        form.getFieldValue(['glossaryTermsParams', 'allowedTerms']) ||
        form.getFieldValue(['glossaryTermsParams', 'resolvedAllowedTerms']);
    const [anyTermsSelected, setAnyTermsSelected] = useState(allowedTerms ? !allowedTerms.length : true);

    function handleAllowedTermsChange(e: RadioChangeEvent) {
        const allowAnyTerm = e.target.value;
        setAnyTermsSelected(allowAnyTerm);
        if (allowAnyTerm) {
            form.setFieldValue(['glossaryTermsParams', 'allowedTerms'], undefined);
        }
    }

    return (
        <>
            <AllowedTermsWrapper>
                <FieldLabel>Allowed Glossary Terms</FieldLabel>
                <StyledRadioGroup value={anyTermsSelected} onChange={handleAllowedTermsChange}>
                    <Space direction="vertical">
                        <Radio value>Any Terms</Radio>
                        <Radio value={false}>Terms in a specific set</Radio>
                    </Space>
                </StyledRadioGroup>
                {!anyTermsSelected && <GlossaryTermsSelector />}
            </AllowedTermsWrapper>
            <CardinalityField paramsField="glossaryTermsParams" />
        </>
    );
}
