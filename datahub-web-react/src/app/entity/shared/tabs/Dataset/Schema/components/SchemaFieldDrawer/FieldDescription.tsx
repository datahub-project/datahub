import { EditOutlined } from '@ant-design/icons';
import { Button, message } from 'antd';
import DOMPurify from 'dompurify';
import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import UpdateDescriptionModal from '@app/entity/shared/components/legacy/DescriptionModal';
import DescriptionSection from '@app/entity/shared/containers/profile/sidebar/AboutSection/DescriptionSection';
import PropagationDetails from '@app/entity/shared/propagation/PropagationDetails';
import { useSchemaRefetch } from '@app/entity/shared/tabs/Dataset/Schema/SchemaContext';
import {
    SectionHeader,
    StyledDivider,
} from '@app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import { getFieldDescriptionDetails } from '@app/entity/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { EditableSchemaFieldInfo, SchemaField, SubResourceType } from '@types';

const EditIcon = styled(Button)`
    border: none;
    box-shadow: none;
    height: 20px;
    width: 20px;
`;

const DescriptionWrapper = styled.div`
    display: flex;
    gap: 4px;
    align-items: center;
    justify-content: space-between;
`;

interface Props {
    expandedField: SchemaField;
    editableFieldInfo?: EditableSchemaFieldInfo;
}

export default function FieldDescription({ expandedField, editableFieldInfo }: Props) {
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [isModalVisible, setIsModalVisible] = useState(false);
    const { entityType } = useEntityData();

    const sendAnalytics = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateSchemaDescription,
            entityType,
            entityUrn: urn,
        });
    };

    const refresh: any = () => {
        refetch?.();
        schemaRefetch?.();
    };

    const onSuccessfulMutation = () => {
        refresh();
        sendAnalytics();
        message.destroy();
        message.success({ content: 'Updated!', duration: 2 });
    };

    const onFailMutation = (e) => {
        message.destroy();
        if (e instanceof Error) message.error({ content: `Proposal Failed! \n ${e.message || ''}`, duration: 2 });
    };

    const generateMutationVariables = (updatedDescription: string) => ({
        variables: {
            input: {
                description: DOMPurify.sanitize(updatedDescription),
                resourceUrn: urn,
                subResource: expandedField.fieldPath,
                subResourceType: SubResourceType.DatasetField,
            },
        },
    });

    const { schemaFieldEntity, description } = expandedField;
    const { displayedDescription, isPropagated, sourceDetail, propagatedDescription } = getFieldDescriptionDetails({
        schemaFieldEntity,
        editableFieldInfo,
        defaultDescription: description,
    });

    const baDescription =
        expandedField?.schemaFieldEntity?.businessAttributes?.businessAttribute?.businessAttribute?.properties
            ?.description;
    const baUrn = expandedField?.schemaFieldEntity?.businessAttributes?.businessAttribute?.businessAttribute?.urn;

    return (
        <>
            <DescriptionWrapper>
                <div>
                    <SectionHeader>Description</SectionHeader>
                    <DescriptionWrapper>
                        {isPropagated && <PropagationDetails sourceDetail={sourceDetail} />}
                        {!!displayedDescription && (
                            <DescriptionSection
                                description={displayedDescription || ''}
                                baDescription={baDescription || ''}
                                baUrn={baUrn || ''}
                                isExpandable
                            />
                        )}
                    </DescriptionWrapper>
                </div>
                {isSchemaEditable && (
                    <EditIcon
                        data-testid="edit-field-description"
                        onClick={() => setIsModalVisible(true)}
                        icon={<EditOutlined />}
                    />
                )}
                {isModalVisible && (
                    <UpdateDescriptionModal
                        title={displayedDescription ? 'Update description' : 'Add description'}
                        description={displayedDescription || ''}
                        original={expandedField.description || ''}
                        onClose={() => setIsModalVisible(false)}
                        onSubmit={(updatedDescription: string) => {
                            message.loading({ content: 'Updating...' });
                            updateDescription(generateMutationVariables(updatedDescription))
                                .then(onSuccessfulMutation)
                                .catch(onFailMutation);
                            setIsModalVisible(false);
                        }}
                        propagatedDescription={propagatedDescription || ''}
                        isAddDesc={!displayedDescription}
                    />
                )}
            </DescriptionWrapper>
            <StyledDivider />
        </>
    );
}
