import React from 'react';
import { Button } from '@src/alchemy-components';
import { Tooltip2 } from '@src/alchemy-components/components/Tooltip2';
import PlatformIcon from '@src/app/sharedV2/icons/PlatformIcon';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import { DataPlatform, EntityPrivileges } from '@src/types.generated';
import { useIncidentURNCopyLink } from '../hooks';
import { IncidentAction, noPermissionsMessage } from '../constant';
import { IncidentTableRow } from '../types';
import {
    ForPlatformWrapper,
    StyledHeader,
    StyledHeaderActions,
    StyledHeaderTitleContainer,
    StyledTitle,
} from './styledComponents';

type IncidentDrawerHeaderProps = {
    mode: IncidentAction;
    onClose?: () => void;
    isEditActive: boolean;
    setIsEditActive: React.Dispatch<React.SetStateAction<boolean>>;
    data?: IncidentTableRow;
    platform?: DataPlatform;
    privileges?: EntityPrivileges;
};

export const IncidentDrawerHeader = ({
    mode,
    onClose,
    isEditActive,
    setIsEditActive,
    data,
    platform,
    privileges,
}: IncidentDrawerHeaderProps) => {
    const handleIncidentLinkCopy = useIncidentURNCopyLink(data ? data?.urn : '');

    const canEditIncidents = privileges?.canEditIncidents || false;

    return (
        <StyledHeader>
            <StyledHeaderTitleContainer>
                <StyledTitle data-testid="drawer-header-title">
                    {mode === IncidentAction.CREATE ? 'Create New Incident' : data?.title}
                </StyledTitle>
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
                        <Tooltip2 title={canEditIncidents ? 'Edit Incident' : noPermissionsMessage}>
                            <span>
                                <Button
                                    icon="PencilSimpleLine"
                                    variant="text"
                                    iconColor="gray"
                                    iconSource="phosphor"
                                    onClick={() => setIsEditActive(!isEditActive)}
                                    disabled={!canEditIncidents}
                                    data-testid="edit-incident-icon"
                                    size="xl"
                                />
                            </span>
                        </Tooltip2>
                        <Tooltip2 title="Copy Link">
                            <Button
                                icon="Link"
                                variant="text"
                                iconColor="gray"
                                iconSource="phosphor"
                                onClick={handleIncidentLinkCopy}
                                size="xl"
                            />
                        </Tooltip2>
                    </>
                )}
                <Button
                    icon="X"
                    variant="text"
                    iconColor="gray"
                    iconSource="phosphor"
                    onClick={() => onClose?.()}
                    data-testid="incident-drawer-close-button"
                    size="xl"
                />
            </StyledHeaderActions>
        </StyledHeader>
    );
};
