import React from 'react';
import styled from 'styled-components';

import { EntitySearchSelect } from '@app/entityV2/shared/EntitySearchSelect/EntitySearchSelect';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { StyledFormItem } from '@app/workflows/components/shared';
import {
    WorkflowRequestFormModalMode,
    getEntitySelectorTitle,
    getPlaceholderText,
} from '@app/workflows/utils/modalHelpers';

import { ActionWorkflowFragment } from '@graphql/actionWorkflow.generated';

const FormSection = styled.div`
    margin-bottom: 24px;
`;

export interface EntitySelectorProps {
    workflow: ActionWorkflowFragment;
    selectedEntityUrn: string;
    onEntityChange: (urn: string) => void;
    mode: WorkflowRequestFormModalMode;
    disabled?: boolean;
}

export const EntitySelector: React.FC<EntitySelectorProps> = ({
    workflow,
    selectedEntityUrn,
    onEntityChange,
    mode,
    disabled = false,
}) => {
    const entityRegistry = useEntityRegistry();

    // Don't render if workflow doesn't require entity selection
    if (!workflow.trigger?.form?.entityTypes || workflow.trigger?.form?.entityTypes.length === 0) {
        return null;
    }

    const entityTitle = getEntitySelectorTitle(workflow, entityRegistry);

    return (
        <FormSection>
            <StyledFormItem
                required
                rules={[{ required: true }]}
                label={entityTitle}
                tooltip={`Select the ${entityTitle.toLowerCase()} this request is associated with`}
            >
                <EntitySearchSelect
                    selectedUrns={selectedEntityUrn ? [selectedEntityUrn] : []}
                    entityTypes={workflow.trigger?.form?.entityTypes || []}
                    isMultiSelect={false}
                    placeholder={getPlaceholderText(mode, `Select ${entityTitle.toLowerCase()}...`)}
                    onUpdate={(urns) => {
                        onEntityChange(urns[0] || '');
                    }}
                    width="full"
                    isDisabled={disabled || mode === WorkflowRequestFormModalMode.REVIEW}
                    data-testid="workflow-entity-selector"
                />
            </StyledFormItem>
        </FormSection>
    );
};
