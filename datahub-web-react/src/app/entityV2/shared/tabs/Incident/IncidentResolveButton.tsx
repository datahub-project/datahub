import React, { useEffect, useState } from 'react';
import { Tooltip } from 'antd';
import { CorpUser, EntityPrivileges, IncidentState } from '@src/types.generated';
import { Button, colors, Pill, Popover } from '@src/alchemy-components';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';
import { Check } from '@phosphor-icons/react';
import { LoadingOutlined } from '@ant-design/icons';
import { useUserContext } from '@src/app/context/useUserContext';
import styled from 'styled-components';

import { ResolverNameContainer } from './styledComponents';
import { IncidentTableRow } from './types';
import { IncidentResolutionPopup } from './IncidentResolutionPopup';
import { LoadingWrapper } from './AcrylComponents/styledComponents';
import { ResolvedSection } from './ResolvedSection';
import { noPermissionsMessage } from './constant';

const ME = 'Me';

const Container = styled.div`
    margin-right: 12px;
    display: flex;
    justify-content: end;
`;

const ResolveButton = styled(Button)`
    margin: 0px;
    padding: 0px;
`;

export const IncidentResolveButton = ({
    incident,
    privileges,
}: {
    incident: IncidentTableRow;
    privileges?: EntityPrivileges;
}) => {
    const canEditIncidents = privileges?.canEditIncidents || false;

    const me = useUserContext();
    const [showResolvePopup, setShowResolvePopup] = useState(false);
    const [incidentResolver, setIncidentResolver] = useState<CorpUser | any>(null);
    const [getAssigneeEntities, { data: resolvedAssigneeEntities, loading }] = useGetEntitiesLazyQuery();
    const resolverName =
        me?.urn === incidentResolver?.urn
            ? ME
            : incidentResolver?.properties?.displayName || incidentResolver?.username;
    useEffect(() => {
        if (incident?.lastUpdated?.actor) {
            getAssigneeEntities({
                variables: {
                    urns: [incident?.lastUpdated?.actor],
                },
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [incident]);

    useEffect(() => {
        if (resolvedAssigneeEntities?.entities?.length) {
            setIncidentResolver(resolvedAssigneeEntities?.entities?.[0]);
        }
    }, [resolvedAssigneeEntities]);

    const handleShowPopup = () => {
        setShowResolvePopup(!showResolvePopup);
    };

    const checkIconRenderer = () => {
        return <Check color="#248F5B" height={9} width={12} />;
    };

    const showPopoverWithResolver = loading ? (
        <LoadingWrapper>
            <LoadingOutlined />
        </LoadingWrapper>
    ) : (
        <ResolverNameContainer>
            <Popover
                content={
                    incidentResolver ? (
                        <ResolvedSection
                            resolverUrn={incidentResolver.urn}
                            resolverName={incidentResolver?.properties?.displayName || incidentResolver?.username}
                            resolverImageUrl={incidentResolver?.editableProperties?.pictureLink}
                            resolverMessage={incident?.message}
                            resolvedDateAndTime={incident?.lastUpdated?.time}
                        />
                    ) : null
                }
                placement="bottom"
            >
                <div>
                    <Pill
                        label={resolverName}
                        clickable={false}
                        customIconRenderer={checkIconRenderer}
                        customStyle={{
                            maxWidth: '100px',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                            overflow: 'hidden',
                            backgroundColor: colors.gray[1300],
                            color: colors.green[1000],
                        }}
                    />
                </div>
            </Popover>
        </ResolverNameContainer>
    );

    return (
        <Container
            onClick={(e) => {
                e.stopPropagation();
            }}
            onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.stopPropagation();
                }
            }}
            tabIndex={0}
            data-testid="incident-resolve-button-container"
        >
            {incident?.state === IncidentState.Active ? (
                <Tooltip showArrow={false} title={!canEditIncidents ? noPermissionsMessage : null}>
                    <ResolveButton disabled={!canEditIncidents} variant="text" onClick={handleShowPopup}>
                        Resolve
                    </ResolveButton>
                </Tooltip>
            ) : (
                showPopoverWithResolver
            )}

            {showResolvePopup && <IncidentResolutionPopup incident={incident} handleClose={handleShowPopup} />}
        </Container>
    );
};
