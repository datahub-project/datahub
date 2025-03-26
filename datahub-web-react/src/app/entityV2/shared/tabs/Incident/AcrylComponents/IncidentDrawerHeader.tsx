import React from 'react';
import { Link as LinkIcon, PencilSimpleLine, X } from '@phosphor-icons/react';
import styled from 'styled-components';
import { Tooltip2 } from '@src/alchemy-components/components/Tooltip2';
import { EntityPrivileges } from '@src/types.generated';
import { useIncidentURNCopyLink } from '../hooks';
import { IncidentAction, noPermissionsMessage } from '../constant';
import { IncidentTableRow } from '../types';
import { StyledHeader, StyledHeaderActions, StyledTitle } from './styledComponents';

const EditIcon = styled(PencilSimpleLine)`
    :hover {
        cursor: pointer;
    }
`;

const CloseIcon = styled(X)`
    :hover {
        cursor: pointer;
    }
`;

const CopyLinkIcon = styled(LinkIcon)`
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
    privileges?: EntityPrivileges;
};

export const IncidentDrawerHeader = ({
    mode,
    onClose,
    isEditActive,
    setIsEditActive,
    data,
    privileges,
}: IncidentDrawerHeaderProps) => {
    const handleIncidentLinkCopy = useIncidentURNCopyLink(data ? data?.urn : '');

    const canEditIncidents = privileges?.canEditIncidents || false;

    return (
        <StyledHeader>
            <StyledTitle>{mode === IncidentAction.ADD ? 'Create New Incident' : data?.title}</StyledTitle>
            <StyledHeaderActions>
                {mode === IncidentAction.VIEW && isEditActive === false && (
                    <>
                        <Tooltip2 title={canEditIncidents ? 'Edit Incident' : noPermissionsMessage}>
                            <EditIcon
                                size={20}
                                onClick={() => canEditIncidents && setIsEditActive(!isEditActive)}
                                data-testid="edit-incident-icon"
                                aria-disabled={!canEditIncidents}
                            />
                        </Tooltip2>
                        <Tooltip2 title="Copy Link">
                            <CopyLinkIcon size={20} onClick={handleIncidentLinkCopy} />
                        </Tooltip2>
                    </>
                )}
                <CloseIcon size={20} onClick={() => onClose?.()} data-testid="incident-drawer-close-button" />
            </StyledHeaderActions>
        </StyledHeader>
    );
};
