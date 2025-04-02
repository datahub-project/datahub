import { GlobalOutlined } from '@ant-design/icons';
import { colors } from '@src/alchemy-components';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { ActionRequest, ActionRequestResult, EntityType } from '@src/types.generated';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import AddContentView from './AddContentView';

const Container = styled.div<{ $isApproved?: boolean }>`
    overflow: hidden;
    display: inline-flex;
    border: 1px ${(props) => (props.$isApproved ? 'solid' : 'dashed')} ${colors.gray[200]};
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
                <StyledGlobalOutlined color={colors.gray[1800]} />
                <DomainName>{entityRegistry.getDisplayName(EntityType.Domain, domain)}</DomainName>
            </Container>
        </Link>
    );

    return <AddContentView requestMetadataViews={[{ primary: domainView }]} actionRequest={actionRequest} />;
};

export default DomainAssociationRequestItem;
