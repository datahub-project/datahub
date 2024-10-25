import { PlusOutlined } from '@ant-design/icons';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import InferenceDetailsIndicator from '@src/app/sharedV2/inferred/InferenceDetailsIndicator';
import { useShouldShowInferDocumentationButton } from '@src/app/entityV2/shared/components/inferredDocs/utils';
import InferDocsButton from '@src/app/entityV2/shared/components/inferredDocs/InferDocsButton';
import { useUpdateDescriptionMutation } from '../../../../../../../../graphql/mutations.generated';
import { useProposeUpdateDescriptionMutation } from '../../../../../../../../graphql/proposals.generated';
import {
    EditableSchemaFieldInfo,
    EntityType,
    SchemaField,
    SubResourceType,
} from '../../../../../../../../types.generated';
import analytics, { EntityActionType, EventType } from '../../../../../../../analytics';
import SchemaEditableContext from '../../../../../../../shared/SchemaEditableContext';
import { useEntityData, useMutationUrn, useRefetch } from '../../../../../../../entity/shared/EntityContext';
import UpdateDescriptionModal from '../../../../../components/legacy/DescriptionModal';
import { REDESIGN_COLORS } from '../../../../../constants';
import DescriptionSection from '../../../../../containers/profile/sidebar/AboutSection/DescriptionSection';
import SectionActionButton from '../../../../../containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '../../../../../containers/profile/sidebar/SidebarSection';
import { useSchemaRefetch } from '../../SchemaContext';
import { StyledDivider } from './components';
import { sanitizeRichText } from '../../../../Documentation/components/editor/utils';
import { getFieldDescriptionDetails } from '../../utils/getFieldDescriptionDetails';
import DocumentationPropagationDetails from '../../../../../../../sharedV2/propagation/DocumentationPropagationDetails';

const AddNewDescription = styled.div`
    margin: 0px;
    padding: 0px;
    color: ${REDESIGN_COLORS.DARK_GREY};
    :hover {
        cursor: pointer;
        color: ${REDESIGN_COLORS.LINK_HOVER_BLUE};
    }
`;

const StyledPlusOutlined = styled(PlusOutlined)`
    && {
        font-size: 10px;
        margin-right: 8px;
    }
`;

const AddDescriptionText = styled.span`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-size: 12px;
    font-weight: 500;
    line-height: 16px;
    :hover {
        color: ${REDESIGN_COLORS.LINK_HOVER_BLUE};
    }
`;

const DescriptionWrapper = styled.div`
    display: flex;
    gap: 4px;
    align-items: center;
`;

interface Props {
    expandedField: SchemaField;
    editableFieldInfo?: EditableSchemaFieldInfo;
    isShowMoreEnabled?: boolean;
}

const PROPOSAL_ENTITY_TYPES = [EntityType.GlossaryTerm, EntityType.GlossaryNode, EntityType.Dataset];

export function getShouldShowProposeButton(entityType: EntityType) {
    return PROPOSAL_ENTITY_TYPES.includes(entityType);
}

export default function FieldDescription({ expandedField, editableFieldInfo, isShowMoreEnabled }: Props) {
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const urn = useMutationUrn();
    const refetch = useRefetch();
    const schemaRefetch = useSchemaRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const [proposeUpdateDescription] = useProposeUpdateDescriptionMutation();
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [inferOnModalVisible, setInferOnModalVisible] = useState(false);
    const { entityType } = useEntityData();
    const shouldShowInferenceButton = useShouldShowInferDocumentationButton(entityType);

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
    const onClose = () => {
        setIsModalVisible(false);
        setInferOnModalVisible(false);
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
    const { displayedDescription, isPropagated, isInferred, sourceDetail, propagatedDescription, inferredDescription } =
        getFieldDescriptionDetails({
            schemaFieldEntity,
            editableFieldInfo,
            defaultDescription: description,
            enableInferredDescriptions: shouldShowInferenceButton,
        });
    const shouldShowProposeButton = getShouldShowProposeButton(entityType);

    return (
        <>
            <SidebarSection
                title="Description"
                extra={
                    isSchemaEditable && (
                        <SectionActionButton
                            onClick={(e) => {
                                e.preventDefault();
                                e.stopPropagation();
                                setIsModalVisible(true);
                            }}
                            button={<EditOutlinedIcon />}
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
                                    <StyledPlusOutlined />
                                    <AddDescriptionText>Add Description</AddDescriptionText>
                                </AddNewDescription>,
                                shouldShowInferenceButton && (
                                    <InferDocsButton
                                        surface="schema-profile"
                                        style={{ height: 32, width: 132, marginTop: 12 }}
                                        onClick={() => {
                                            setInferOnModalVisible(true);
                                            setIsModalVisible(true);
                                        }}
                                    />
                                ),
                            ]}
                        <DescriptionWrapper>
                            {isPropagated && <DocumentationPropagationDetails sourceDetail={sourceDetail} />}
                            {isInferred && <InferenceDetailsIndicator />}
                            {!!displayedDescription && (
                                <DescriptionSection
                                    description={displayedDescription}
                                    isShowMoreEnabled={isShowMoreEnabled}
                                    isExpandable
                                />
                            )}
                        </DescriptionWrapper>
                    </>
                }
            />
            {isModalVisible && (
                <UpdateDescriptionModal
                    title={displayedDescription ? 'Update description' : 'Add description'}
                    fieldPath={expandedField.fieldPath}
                    description={displayedDescription || ''}
                    original={expandedField.description || ''}
                    propagatedDescription={propagatedDescription || ''}
                    inferredDescription={inferredDescription || ''}
                    onClose={onClose}
                    onSubmit={(updatedDescription: string) => {
                        message.loading({ content: 'Updating...' });
                        updateDescription(generateMutationVariables(updatedDescription))
                            .then(onSuccessfulMutation)
                            .catch(onFailMutation);
                        onClose();
                    }}
                    showPropose={shouldShowProposeButton}
                    onPropose={(updatedDescription) => {
                        message.loading({ content: 'Updating...' });
                        proposeUpdateDescription(generateMutationVariables(updatedDescription))
                            .then(onSuccessfulMutation)
                            .catch(onFailMutation);
                        onClose();
                    }}
                    isAddDesc={!displayedDescription}
                    inferOnMount={inferOnModalVisible}
                />
            )}
            <StyledDivider dashed />
        </>
    );
}
