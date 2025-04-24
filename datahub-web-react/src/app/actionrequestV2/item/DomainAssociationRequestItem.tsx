import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import AddContentView from '@app/actionrequestV2/item/AddContentView';
import { colors } from '@src/alchemy-components';
import { DomainContent } from '@src/app/sharedV2/tags/DomainLink';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { ActionRequest, ActionRequestResult, EntityType } from '@src/types.generated';

const Container = styled.div<{ $isApproved?: boolean }>`
    overflow: hidden;
    display: inline-flex;
    border: 1px ${(props) => (props.$isApproved ? 'solid' : 'dashed')} ${colors.gray[200]};
    border-radius: 12px;
    padding: 3px 6px 3px 3px;
    align-items: center;
    font-size: 12px;
`;

interface Props {
    actionRequest: ActionRequest;
}

const DomainAssociationRequestItem = ({ actionRequest }: Props) => {
    const entityRegistry = useEntityRegistry();

    const domain = actionRequest.params?.domainProposal?.domain;

    // Don't render if the domain is empty
    if (!domain || !domain.urn) {
        return null;
    }

    const domainView = domain && Object.keys(domain).length && (
        <Link to={`/${entityRegistry.getPathName(EntityType.Domain)}/${domain.urn}`}>
            <Container $isApproved={actionRequest.result === ActionRequestResult.Accepted}>
                <DomainContent
                    domain={domain}
                    name={entityRegistry.getDisplayName(EntityType.Domain, domain)}
                    closable={false}
                    iconSize={24}
                />
            </Container>
        </Link>
    );

    return <AddContentView requestMetadataViews={[{ primary: domainView }]} actionRequest={actionRequest} />;
};

export default DomainAssociationRequestItem;
