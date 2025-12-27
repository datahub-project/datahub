import React from 'react';
import styled from 'styled-components/macro';
import { Tooltip } from '@components';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { EntityType } from '@types';

const PillContainer = styled.div<{ clickable?: boolean }>`
    display: inline-flex;
    align-items: center;
    background-color: #f5f5f5;
    border-radius: 16px;
    padding: 4px 12px;
    font-size: 12px;
    font-weight: 500;
    color: #262626;
    cursor: ${(props) => (props.clickable ? 'pointer' : 'default')};
    max-width: 100%;
    
    &:hover {
        background-color: ${(props) => (props.clickable ? '#e8e8e8' : '#f5f5f5')};
    }
`;

const Name = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 150px;
`;

const CloseButton = styled.span`
    margin-left: 6px;
    cursor: pointer;
    opacity: 0.6;
    font-size: 14px;
    line-height: 1;
    
    &:hover {
        opacity: 1;
    }
`;

interface Props {
    organizationUrn: string;
    organizationName: string;
    onClose?: (e: React.MouseEvent) => void;
}

const OrganizationPill: React.FC<Props> = ({ organizationUrn, organizationName, onClose }) => {
    const entityRegistry = useEntityRegistry();
    const organizationUrl = entityRegistry.getEntityUrl(EntityType.Organization, organizationUrn);

    const handleClick = (e: React.MouseEvent) => {
        if (!onClose) {
            e.preventDefault();
            window.location.href = organizationUrl;
        }
    };

    return (
        <Tooltip title={organizationName}>
            <PillContainer clickable={!onClose} onClick={handleClick}>
                <Name>{organizationName}</Name>
                {onClose && <CloseButton onClick={onClose}>×</CloseButton>}
            </PillContainer>
        </Tooltip>
    );
};

export default OrganizationPill;
