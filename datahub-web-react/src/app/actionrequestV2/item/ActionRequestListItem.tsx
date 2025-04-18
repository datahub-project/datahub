import { ActionRequest, ActionRequestType } from '@src/types.generated';
import React from 'react';
import TermAssociationRequestItem from './TermAssociationRequestItem';
import TagAssociationRequestItem from './TagAssociationRequestItem';
import CreateTermRequestItem from './CreateTermRequestItem';
import CreateNodeRequestItem from './CreateNodeRequestItem';
import UpdateDescriptionRequestItem from './updateDescription/UpdateDescriptionRequestItem';
import StructuredPropertyAssociationRequestItem from './StructuredPropertyAsssociationRequestItem';
import CreateDataContractRequestItem from './CreateDataContractRequestItem';
import DomainAssociationRequestItem from './DomainAssociationRequestItem';
import OwnerAssociationRequestItem from './OwnerAssociationRequestItem';

interface Props {
    actionRequest: ActionRequest;
}

export const ActionRequestListItem = ({ actionRequest }: Props) => {
    const getActionRequestItemContent = (request: ActionRequest) => {
        const requestType = request.type;
        let contentView;
        switch (requestType) {
            case ActionRequestType.TermAssociation: {
                contentView = <TermAssociationRequestItem actionRequest={actionRequest} />;
                break;
            }
            case ActionRequestType.TagAssociation: {
                contentView = <TagAssociationRequestItem actionRequest={actionRequest} />;
                break;
            }
            case ActionRequestType.CreateGlossaryTerm: {
                contentView = <CreateTermRequestItem actionRequest={actionRequest} />;
                break;
            }
            case ActionRequestType.CreateGlossaryNode: {
                contentView = <CreateNodeRequestItem actionRequest={actionRequest} />;

                break;
            }
            case ActionRequestType.UpdateDescription: {
                contentView = <UpdateDescriptionRequestItem actionRequest={actionRequest} />;
                break;
            }
            case ActionRequestType.DataContract: {
                contentView = <CreateDataContractRequestItem actionRequest={actionRequest} />;
                break;
            }
            case ActionRequestType.StructuredPropertyAssociation: {
                contentView = <StructuredPropertyAssociationRequestItem actionRequest={actionRequest} />;
                break;
            }
            case ActionRequestType.DomainAssociation: {
                contentView = <DomainAssociationRequestItem actionRequest={actionRequest} />;
                break;
            }
            case ActionRequestType.OwnerAssociation: {
                contentView = <OwnerAssociationRequestItem actionRequest={actionRequest} />;
                break;
            }
            default:
                console.error(`Unrecognized Action Request Type ${requestType} provided. Unable to render.`);
                return null;
        }
        return contentView;
    };
    return <div>{getActionRequestItemContent(actionRequest)}</div>;
};
