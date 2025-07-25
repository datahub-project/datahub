import { TextArea } from '@components';
import { Form } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { WorkflowRequestFormModalMode, getPlaceholderText } from '@app/workflows/utils/modalHelpers';

const FormSection = styled.div`
    margin-bottom: 24px;
`;

export interface DescriptionFieldProps {
    description: string;
    onDescriptionChange: (description: string) => void;
    mode: WorkflowRequestFormModalMode;
    disabled?: boolean;
}

export const DescriptionField: React.FC<DescriptionFieldProps> = ({
    description,
    onDescriptionChange,
    mode,
    disabled = false,
}) => {
    return (
        <FormSection>
            <Form.Item label="Additional Notes">
                <TextArea
                    label=""
                    value={description}
                    onChange={(e) => onDescriptionChange(e.target.value)}
                    placeholder={getPlaceholderText(mode, "Any other context you'd like to add?")}
                    rows={3}
                    disabled={disabled || mode === WorkflowRequestFormModalMode.REVIEW}
                    data-testid="workflow-description"
                />
            </Form.Item>
        </FormSection>
    );
};
