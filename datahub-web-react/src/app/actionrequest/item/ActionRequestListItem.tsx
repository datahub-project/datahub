import { Checkbox, List } from 'antd';
import React from 'react';
import styled from 'styled-components';

import CreateNodeRequestItem from '@app/actionrequest/item/CreateNodeRequestItem';
import CreateTermRequestItem from '@app/actionrequest/item/CreateTermRequestItem';
import DataContractListItem from '@app/actionrequest/item/DataContractListItem';
import DomainAssociationRequestItem from '@app/actionrequest/item/DomainAssociationRequestItem';
import OwnerAssociationRequestItem from '@app/actionrequest/item/OwnerAssociationRequestItem';
import StructuredPropertyAsssociationRequestItem from '@app/actionrequest/item/StructuredPropertyAsssociationRequestItem';
import TagAssociationRequestItem from '@app/actionrequest/item/TagAssociationRequestItem';
import TermAssociationRequestItem from '@app/actionrequest/item/TermAssociationRequestItem';
import UpdateDescriptionRequestItem from '@app/actionrequest/item/updateDescription/UpdateDescriptionRequestItem';

import { ActionRequest, ActionRequestType } from '@types';

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
            case ActionRequestType.DomainAssociation:
                return (
                    <DomainAssociationRequestItem
                        actionRequest={request}
                        onUpdate={onUpdate}
                        showActionsButtons={showActionsButtons}
                    />
                );
            case ActionRequestType.OwnerAssociation:
                return (
                    <OwnerAssociationRequestItem
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
