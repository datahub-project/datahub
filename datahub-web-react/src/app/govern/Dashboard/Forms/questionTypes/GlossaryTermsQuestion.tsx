import { Form, Tooltip } from 'antd';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import React, { useState } from 'react';
import { AllowedItemsWrapper, StyledCheckbox, StyledLabel } from '../styledComponents';
import CardinalityField from './CardinalityField';
import GlossaryTermsSelector from './GlossaryTermsSelector';

export default function GlossaryTermsQuestion() {
    const form = Form.useFormInstance();
    const allowedTerms =
        form.getFieldValue(['glossaryTermsParams', 'allowedTerms']) ||
        form.getFieldValue(['glossaryTermsParams', 'resolvedAllowedTerms']);
    const [anyTermsSelected, setAnyTermsSelected] = useState(allowedTerms ? !allowedTerms.length : true);

    function handleAllowedTermsChange(e: CheckboxChangeEvent) {
        const allowAnyTerm = !e.target.checked;
        setAnyTermsSelected(allowAnyTerm);
        if (allowAnyTerm) {
            form.setFieldValue(['glossaryTermsParams', 'allowedTerms'], undefined);
            form.setFieldValue(['glossaryTermsParams', 'resolvedAllowedTerms'], undefined);
        }
    }

    return (
        <>
            <CardinalityField paramsField="glossaryTermsParams" inputType="glossary terms" />
            <AllowedItemsWrapper>
                <StyledCheckbox checked={!anyTermsSelected} onChange={handleAllowedTermsChange} />
                <Tooltip title="If left unchecked, then any glossary term will be allowed" showArrow={false}>
                    <StyledLabel>Restrict responses to specific glossary terms</StyledLabel>
                </Tooltip>
                {!anyTermsSelected && <GlossaryTermsSelector />}
            </AllowedItemsWrapper>
        </>
    );
}
