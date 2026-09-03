import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import PillRemoveIcon from '@app/sharedV2/icons/PillRemoveIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Application, EntityType } from '@types';

const StyledLink = styled(Link)<{ $fontSize?: number }>`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    color: ${(props) => props.theme.colors.text};
    ${(props) => props.$fontSize && `font-size: ${props.$fontSize}px;`}

    &:hover,
    &:focus,
    &:active {
        color: ${(props) => props.theme.colors.text};
    }
`;

const IconWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    color: ${(props) => props.theme.colors.icon};
`;

const RemoveIcon = styled(PillRemoveIcon)`
    margin-left: 4px;
`;

interface Props {
    application: Application;
    closable?: boolean;
    onClose?: (e) => void;
    readOnly?: boolean;
    fontSize?: number;
}

export const ApplicationLink = ({ application, closable, onClose, readOnly, fontSize }: Props) => {
    const entityRegistry = useEntityRegistry();
    const applicationPath = entityRegistry.getPathName(application.type);
    const applicationUrl = `/${applicationPath}/${encodeURIComponent(application.urn)}`;

    return (
        <StyledLink to={applicationUrl} $fontSize={fontSize} data-testid={`application-${application.urn}`}>
            <IconWrapper>{entityRegistry.getIcon(EntityType.Application, 16, IconStyleType.ACCENT)}</IconWrapper>
            {application.properties?.name || application.urn}
            {!readOnly && closable && <RemoveIcon onClick={onClose} />}
        </StyledLink>
    );
};
