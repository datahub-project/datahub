import React from 'react';
import { Link as LinkIcon, PencilSimpleLine, X } from '@phosphor-icons/react';
import styled from 'styled-components';
import { Tooltip2 } from '@src/alchemy-components/components/Tooltip2';
import { useIncidentURNCopyLink } from '../hooks';
import { IncidentAction } from '../constant';
import { IncidentTableRow } from '../types';
import { StyledHeader, StyledHeaderActions, StyledTitle } from './styledComponents';

const EditButton = styled(PencilSimpleLine)`
    :hover {
        cursor: pointer;
    }
`;

const CloseButton = styled(X)`
    :hover {
        cursor: pointer;
    }
`;

const LinkButton = styled(LinkIcon)`
    :hover {
        cursor: pointer;
    }
`;

type IncidentDrawerHeaderProps = {
    mode: IncidentAction;
    onClose?: () => void;
    isEditActive: boolean;
    setIsEditActive: React.Dispatch<React.SetStateAction<boolean>>;
    data?: IncidentTableRow;
};

export const IncidentDrawerHeader = ({
    mode,
    onClose,
    isEditActive,
    setIsEditActive,
    data,
}: IncidentDrawerHeaderProps) => {
    const handleIncidentLinkCopy = useIncidentURNCopyLink(data ? data?.urn : '');
    return (
        <StyledHeader>
            <StyledTitle>{mode === IncidentAction.CREATE ? 'Create New Incident' : data?.title}</StyledTitle>
            <StyledHeaderActions>
                {mode === IncidentAction.EDIT && isEditActive === false && (
                    <>
                        <Tooltip2 title="Edit Incident">
                            <EditButton
                                size={20}
                                onClick={() => setIsEditActive(!isEditActive)}
                                data-testid="edit-incident-icon"
                            />
                        </Tooltip2>
                        <Tooltip2 title="Copy Link">
                            <LinkButton size={20} onClick={handleIncidentLinkCopy} />
                        </Tooltip2>
                    </>
                )}
                <CloseButton size={20} onClick={() => onClose?.()} data-testid="incident-drawer-close-button" />
            </StyledHeaderActions>
        </StyledHeader>
    );
};
