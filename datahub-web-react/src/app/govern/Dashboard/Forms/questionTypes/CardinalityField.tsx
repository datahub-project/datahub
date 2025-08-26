import { Form } from 'antd';
import React, { useState } from 'react';

import { StyledCheckbox, StyledLabel } from '@app/govern/Dashboard/Forms/styledComponents';
import { PromptCardinality } from '@src/types.generated';

interface Props {
    paramsField: string;
    inputType: string;
}

const CardinalityField = ({ paramsField, inputType }: Props) => {
    const form = Form.useFormInstance();
    const cardinality = form.getFieldValue([paramsField, 'cardinality']) || PromptCardinality.Multiple;
    const [isChecked, setIsChecked] = useState(cardinality === PromptCardinality.Multiple);

    function handleCheck() {
        const newCardinality =
            cardinality === PromptCardinality.Single ? PromptCardinality.Multiple : PromptCardinality.Single;
        form.setFieldValue([paramsField, 'cardinality'], newCardinality);
        setIsChecked(!isChecked);
    }

    return (
        <>
            <Form.Item name={[paramsField, 'cardinality']} style={{ minHeight: 'max-content' }}>
                <StyledCheckbox checked={isChecked} onChange={handleCheck} />
                <StyledLabel onClick={handleCheck}>Allow multiple {inputType} </StyledLabel>
            </Form.Item>
        </>
    );
};

export default CardinalityField;
