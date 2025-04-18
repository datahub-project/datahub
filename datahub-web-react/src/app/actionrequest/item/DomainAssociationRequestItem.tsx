import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { colors } from '@src/alchemy-components';
import { GlobalOutlined } from '@ant-design/icons';
import MetadataAssociationRequestItem from './MetadataAssociationRequestItem';
import AddContentView from './AddContentView';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ActionRequest, EntityType } from '../../../types.generated';

type Props = {
    actionRequest: ActionRequest;
    onUpdate: () => void;
    showActionsButtons: boolean;
};

const Container = styled.div`
    overflow: hidden;
    display: inline-flex;
    border: 1px solid ${colors.gray[100]};
    border-radius: 200px;
    padding: 2px 8px 2px 4px;
    align-items: center;
    font-size: 12px;
`;

const StyledGlobalOutlined = styled(GlobalOutlined)<{ color }>`
    color: ${(props) => props.color};
    margin-right: 4px;
`;

const DomainName = styled.div`
    max-width: 180px;
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
    color: ${colors.gray[1700]};
`;

const REQUEST_TYPE_DISPLAY_NAME = 'Domain Proposal';

/**
 * A list item representing a Domain association request.
 */
export default function DomainAssociationRequestItem({ actionRequest, onUpdate, showActionsButtons }: Props) {
    const entityRegistry = useEntityRegistry();

    const domain = actionRequest.params?.domainProposal?.domain;

    // Don't render if the domain is empty
    if (!domain || !domain.urn) {
        return null;
    }

    const domainView = domain && Object.keys(domain).length && (
        <Link to={`/${entityRegistry.getPathName(EntityType.Domain)}/${domain.urn}`}>
            <Container>
                <StyledGlobalOutlined color={colors.gray[1800]} />
                <DomainName>{entityRegistry.getDisplayName(EntityType.Domain, domain)}</DomainName>
            </Container>
        </Link>
    );

    const contentView = (
        <AddContentView requestMetadataViews={[{ primary: domainView }]} actionRequest={actionRequest} />
    );

    return (
        <MetadataAssociationRequestItem
            requestTypeDisplayName={REQUEST_TYPE_DISPLAY_NAME}
            requestContentView={contentView}
            actionRequest={actionRequest}
            onUpdate={onUpdate}
            showActionsButtons={showActionsButtons}
        />
    );
}
