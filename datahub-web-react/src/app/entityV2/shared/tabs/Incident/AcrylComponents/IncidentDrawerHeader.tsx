import React from 'react';
import styled from 'styled-components';
import { Link as LinkIcon, PencilSimpleLine, X } from '@phosphor-icons/react';
import { Tooltip2 } from '@src/alchemy-components/components/Tooltip2';
import PlatformIcon from '@src/app/sharedV2/icons/PlatformIcon';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import { DataPlatform } from '@src/types.generated';
import { useIncidentURNCopyLink } from '../hooks';
import { IncidentAction } from '../constant';
import { IncidentTableRow } from '../types';
import {
    ForPlatformWrapper,
    StyledHeader,
    StyledHeaderActions,
    StyledHeaderTitleContainer,
    StyledTitle,
} from './styledComponents';

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
    platform?: DataPlatform;
};

export const IncidentDrawerHeader = ({
    mode,
    onClose,
    isEditActive,
    setIsEditActive,
    data,
    platform,
}: IncidentDrawerHeaderProps) => {
    const handleIncidentLinkCopy = useIncidentURNCopyLink(data ? data?.urn : '');
    return (
        <StyledHeader>
            <StyledHeaderTitleContainer>
                <StyledTitle>{mode === IncidentAction.ADD ? 'Create New Incident' : data?.title}</StyledTitle>
                {platform && (
                    <ForPlatformWrapper>
                        <PlatformIcon platform={platform} size={16} styles={{ marginRight: 4 }} />
                        {capitalizeFirstLetter(platform.name)}
                    </ForPlatformWrapper>
                )}
            </StyledHeaderTitleContainer>
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
