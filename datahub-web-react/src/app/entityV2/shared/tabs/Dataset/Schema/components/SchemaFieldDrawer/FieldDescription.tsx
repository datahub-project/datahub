import { Icon, Tooltip, toast } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useState } from 'react';
import styled from 'styled-components';

import { EditorProps } from '@components/components/Editor/types';
import { sanitizeRichText } from '@components/components/Editor/utils';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import UpdateDescriptionModal from '@app/entityV2/shared/components/legacy/DescriptionModal';
import DescriptionSection from '@app/entityV2/shared/containers/profile/sidebar/AboutSection/DescriptionSection';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { useSchemaRefetch } from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaContext';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import { getFieldDescriptionDetails } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';
import HoverCardAttributionDetails from '@app/sharedV2/propagation/HoverCardAttributionDetails';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { EditableSchemaFieldInfo, SchemaField, SubResourceType } from '@types';

const AddNewDescription = styled.div`
    margin: 0px;
    padding: 0px;
    color: ${(props) => props.theme.colors.textSecondary};
    :hover {
        cursor: pointer;
        color: ${(props) => props.theme.colors.textBrand};
    }
`;

const AddDescriptionText = styled.span`
    font-size: 12px;
    font-weight: 500;
    line-height: 16px;
`;

const DescriptionWrapper = styled.div`
    display: flex;
    gap: 4px;
    align-items: center;
`;

interface Props {
    expandedField: SchemaField;
    editableFieldInfo?: EditableSchemaFieldInfo;
    editorProps?: Partial<EditorProps>;
}

export default function FieldDescription({ expandedField, editableFieldInfo, editorProps }: Props) {
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
        toast.destroy();
        toast.success('Updated!', { duration: 2 });
    };

    const onFailMutation = (e) => {
        toast.destroy();
        if (e instanceof Error) toast.error(`Proposal Failed! \n ${e.message || ''}`, { duration: 2 });
    };
    const onClose = () => {
        setIsModalVisible(false);
    };

    const generateMutationVariables = (updatedDescription: string) => {
        return {
            variables: {
                input: {
                    description: sanitizeRichText(updatedDescription),
                    resourceUrn: urn,
                    subResource: expandedField.fieldPath,
                    subResourceType: SubResourceType.DatasetField,
                },
            },
        };
    };

    const { schemaFieldEntity, description } = expandedField;
    const { displayedDescription, isPropagated, propagatedDescription, attribution } = getFieldDescriptionDetails({
        schemaFieldEntity,
        editableFieldInfo,
        defaultDescription: description,
    });

    return (
        <>
            <SidebarSection
                title="Description"
                extra={
                    isSchemaEditable && (
                        <SectionActionButton
                            dataTestId="edit-field-description"
                            onClick={(e) => {
                                e.preventDefault();
                                e.stopPropagation();
                                setIsModalVisible(true);
                            }}
                            icon={PencilSimple}
                        />
                    )
                }
                content={
                    <>
                        {!displayedDescription &&
                            isSchemaEditable && [
                                <AddNewDescription
                                    onClick={() => {
                                        setIsModalVisible(true);
                                    }}
                                >
                                    <Icon icon={Plus} size="sm" />
                                    <AddDescriptionText>Add Description</AddDescriptionText>
                                </AddNewDescription>,
                            ]}
                        {!!displayedDescription && (
                            <Tooltip
                                title={
                                    isPropagated && <HoverCardAttributionDetails propagationDetails={{ attribution }} />
                                }
                            >
                                <DescriptionWrapper>
                                    <DescriptionSection description={displayedDescription} isExpandable />
                                </DescriptionWrapper>
                            </Tooltip>
                        )}
                    </>
                }
            />
            {isModalVisible && (
                <UpdateDescriptionModal
                    title={displayedDescription ? 'Update description' : 'Add description'}
                    description={displayedDescription || ''}
                    original={expandedField.description || ''}
                    propagatedDescription={propagatedDescription || ''}
                    onClose={onClose}
                    onSubmit={(updatedDescription: string) => {
                        toast.loading('Updating...');
                        updateDescription(generateMutationVariables(updatedDescription))
                            .then(onSuccessfulMutation)
                            .catch(onFailMutation);
                        onClose();
                    }}
                    isAddDesc={!displayedDescription}
                    editorProps={editorProps}
                />
            )}
            <StyledDivider dashed />
        </>
    );
}
