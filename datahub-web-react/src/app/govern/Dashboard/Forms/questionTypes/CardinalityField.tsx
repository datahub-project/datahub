import { PromptCardinality } from '@src/types.generated';
import { Form } from 'antd';
import React, { useState } from 'react';
import { FieldLabel, StyledCheckbox, StyledLabel } from '../styledComponents';

interface Props {
    paramsField: string;
}

const CardinalityField = ({ paramsField }: Props) => {
    const form = Form.useFormInstance();
    const cardinality = form.getFieldValue([paramsField, 'cardinality']) || PromptCardinality.Single;
    const [isChecked, setIsChecked] = useState(cardinality === PromptCardinality.Multiple);

    function handleCheck() {
        const newCardinality =
            cardinality === PromptCardinality.Single ? PromptCardinality.Multiple : PromptCardinality.Single;
        form.setFieldValue([paramsField, 'cardinality'], newCardinality);
        setIsChecked(!isChecked);
    }

    return (
        <>
            <FieldLabel style={{ marginBottom: 0 }}> Allow Multiple</FieldLabel>
            <Form.Item name={[paramsField, 'cardinality']} style={{ minHeight: 'max-content' }}>
                <StyledCheckbox checked={isChecked} onChange={handleCheck} />
                <StyledLabel onClick={handleCheck}>Check to allow multiple</StyledLabel>
            </Form.Item>
        </>
    );
};

export default CardinalityField;
