import { DatePicker } from '@components';
import { Form } from 'antd';
import moment from 'moment';
import React from 'react';
import styled from 'styled-components';

import {
    WorkflowRequestFormModalMode,
    getPlaceholderText,
    shouldShowAccessFields,
} from '@app/workflows/utils/modalHelpers';

import { ActionWorkflowFragment } from '@graphql/actionWorkflow.generated';

const FormSection = styled.div`
    margin-bottom: 24px;
`;

export interface AccessFieldsProps {
    workflow: ActionWorkflowFragment;
    expiresAt?: number;
    onExpiresAtChange: (timestamp?: number) => void;
    mode: WorkflowRequestFormModalMode;
    disabled?: boolean;
}

export const AccessFields: React.FC<AccessFieldsProps> = ({
    workflow,
    expiresAt,
    onExpiresAtChange,
    mode,
    disabled = false,
}) => {
    // Don't render if workflow doesn't show access fields
    if (!shouldShowAccessFields(workflow)) {
        return null;
    }

    return (
        <FormSection>
            <Form.Item
                label={<>Expiration Date</>}
                tooltip="When should this access expire? Leave blank for no expiration."
            >
                <DatePicker
                    value={expiresAt ? moment(expiresAt) : null}
                    onChange={(date) => onExpiresAtChange(date ? date.valueOf() : undefined)}
                    disabled={disabled || mode === WorkflowRequestFormModalMode.REVIEW}
                    placeholder={getPlaceholderText(mode, 'Select date')}
                    data-testid="workflow-expiration-date"
                />
            </Form.Item>
        </FormSection>
    );
};
