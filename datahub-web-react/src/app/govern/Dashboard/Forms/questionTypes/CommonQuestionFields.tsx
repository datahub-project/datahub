import { Input, TextArea } from '@src/alchemy-components';
import { WARNING_COLOR_HEX } from '@src/app/entityV2/shared/tabs/Incident/incidentUtils';
import { Form, Radio } from 'antd';
import React, { useEffect, useState } from 'react';
import { FieldLabel, StyledExclamationOutlined, StyledRadioGroup, WarningWarpper } from '../styledComponents';

interface Props {
    isFormDisabled: boolean;
}

const CommonQuestionFields = ({ isFormDisabled }: Props) => {
    const form = Form.useFormInstance();
    const questionType = form.getFieldValue('type') || '';
    const required = form.getFieldValue('required') || !questionType.startsWith('FIELD');
    const [isRequired, setIsRequired] = useState(required || !questionType.startsWith('FIELD'));

    useEffect(() => {
        setIsRequired(required);
    }, [required]);

    return (
        <>
            <Form.Item
                name="title"
                rules={[
                    {
                        required: true,
                        message: 'Please enter the question',
                    },
                ]}
            >
                <Input placeholder="Add Question here" label="Title" />
            </Form.Item>
            <Form.Item name="description">
                <TextArea label="Description" placeholder="Add description here" isDisabled={isFormDisabled} />
            </Form.Item>
            <FieldLabel> Mandatory</FieldLabel>
            <Form.Item name="required">
                <StyledRadioGroup>
                    <Radio value onClick={() => setIsRequired(true)}>
                        Yes
                    </Radio>
                    <Radio value={false} onClick={() => setIsRequired(false)}>
                        No
                    </Radio>
                </StyledRadioGroup>
            </Form.Item>
            {isRequired && questionType.startsWith('FIELD') && (
                <WarningWarpper>
                    <StyledExclamationOutlined color={WARNING_COLOR_HEX} />
                    <span>
                        <strong>Are you sure?</strong> All columns will need an anwer to this question individually to
                        complete the form.
                    </span>
                </WarningWarpper>
            )}
        </>
    );
};

export default CommonQuestionFields;
