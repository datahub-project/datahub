import { Checkbox, List } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ActionRequest, ActionRequestType } from '../../../types.generated';
import CreateNodeRequestItem from './CreateNodeRequestItem';
import CreateTermRequestItem from './CreateTermRequestItem';
import DataContractListItem from './DataContractListItem';
import StructuredPropertyAsssociationRequestItem from './StructuredPropertyAsssociationRequestItem';
import TagAssociationRequestItem from './TagAssociationRequestItem';
import TermAssociationRequestItem from './TermAssociationRequestItem';
import UpdateDescriptionRequestItem from './updateDescription/UpdateDescriptionRequestItem';

const ActionRequestItemContainer = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
`;

const CheckboxContainer = styled.div`
    display: flex;
    align-items: center;
`;

type Props = {
    actionRequest: ActionRequest;
    onUpdate: () => void;
    showActionsButtons: boolean;
    selectable?: boolean;
    selected?: boolean;
    onSelect?: () => void;
};

/**
 * Base Action Request List Item. Each specific action request type has it's own way
 * to render the item, which is handled inside this component.
 */
export default function ActionRequestListItem({
    actionRequest,
    showActionsButtons,
    selectable = false,
    selected = false,
    onSelect,
    onUpdate,
}: Props) {
    const getActionRequestItemContent = (request: ActionRequest) => {
        const requestType = request.type;
        switch (requestType) {
            // Request to add a glossary term to an entity.
            case ActionRequestType.TermAssociation:
                return (
                    <TermAssociationRequestItem
                        actionRequest={request}
                        onUpdate={onUpdate}
                        showActionsButtons={showActionsButtons}
                    />
                );
            // Request to add a tag to an entity.
            case ActionRequestType.TagAssociation:
                return (
                    <TagAssociationRequestItem
                        actionRequest={request}
                        onUpdate={onUpdate}
                        showActionsButtons={showActionsButtons}
                    />
                );
            case ActionRequestType.CreateGlossaryTerm:
                return (
                    <CreateTermRequestItem
                        actionRequest={request}
                        onUpdate={onUpdate}
                        showActionsButtons={showActionsButtons}
                    />
                );
            case ActionRequestType.CreateGlossaryNode:
                return (
                    <CreateNodeRequestItem
                        actionRequest={request}
                        onUpdate={onUpdate}
                        showActionsButtons={showActionsButtons}
                    />
                );
            case ActionRequestType.UpdateDescription:
                return (
                    <UpdateDescriptionRequestItem
                        actionRequest={request}
                        onUpdate={onUpdate}
                        showActionsButtons={showActionsButtons}
                    />
                );
            case ActionRequestType.DataContract:
                return (
                    <DataContractListItem
                        actionRequest={request}
                        onUpdate={onUpdate}
                        showActionsButtons={showActionsButtons}
                    />
                );
            case ActionRequestType.StructuredPropertyAssociation:
                return (
                    <StructuredPropertyAsssociationRequestItem
                        actionRequest={request}
                        onUpdate={onUpdate}
                        showActionsButtons={showActionsButtons}
                    />
                );
            default:
                console.error(`Unrecognized Action Request Type ${requestType} provided. Unable to render.`);
                return null;
        }
    };

    const actionRequestItemContent = getActionRequestItemContent(actionRequest);

    return (
        <List.Item>
            {/* test id is being provided as a classname here so the number of action requests can be counted */}
            <ActionRequestItemContainer className="action-request-test-id">
                {selectable && (
                    <CheckboxContainer>
                        <Checkbox checked={selected} onChange={onSelect} />
                    </CheckboxContainer>
                )}
                {actionRequestItemContent}
            </ActionRequestItemContainer>
        </List.Item>
    );
}
