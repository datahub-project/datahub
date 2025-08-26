import React from 'react';

import CreateDataContractRequestItem from '@app/actionrequestV2/item/CreateDataContractRequestItem';
import CreateNodeRequestItem from '@app/actionrequestV2/item/CreateNodeRequestItem';
import CreateTermRequestItem from '@app/actionrequestV2/item/CreateTermRequestItem';
import DomainAssociationRequestItem from '@app/actionrequestV2/item/DomainAssociationRequestItem';
import OwnerAssociationRequestItem from '@app/actionrequestV2/item/OwnerAssociationRequestItem';
import StructuredPropertyAssociationRequestItem from '@app/actionrequestV2/item/StructuredPropertyAsssociationRequestItem';
import TagAssociationRequestItem from '@app/actionrequestV2/item/TagAssociationRequestItem';
import TermAssociationRequestItem from '@app/actionrequestV2/item/TermAssociationRequestItem';
import UpdateDescriptionRequestItem from '@app/actionrequestV2/item/updateDescription/UpdateDescriptionRequestItem';
import { ActionRequest, ActionRequestType } from '@src/types.generated';

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
