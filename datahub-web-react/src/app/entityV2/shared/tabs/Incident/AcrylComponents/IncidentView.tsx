import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { IncidentStagePill } from '@src/alchemy-components/components/IncidentStagePill';
import { getCapitalizeWord } from '@src/alchemy-components/components/IncidentStagePill/utils';
import { EntityLinkList } from '@src/app/homeV2/reference/sections/EntityLinkList';
import { IncidentPriorityLabel } from '@src/alchemy-components/components/IncidentPriorityLabel';
import { Avatar } from '@src/alchemy-components';
import {
    Assertion,
    AssertionInfo,
    CorpUser,
    EntityType,
    IncidentSourceType,
    IncidentState,
} from '@src/types.generated';
import { Check, Warning } from '@phosphor-icons/react';
import { IconLabel } from '@src/alchemy-components/components/IconLabel';
import { IconType } from '@src/alchemy-components/components/IconLabel/types';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import {
    CategoryText,
    Container,
    DescriptionSection,
    DetailsLabel,
    DetailsSection,
    Divider,
    Header,
    ListContainer,
    ListItemContainer,
    Text,
} from './styledComponents';
import CompactMarkdownViewer from '../../Documentation/components/CompactMarkdownViewer';
import { getAssigneeNamesWithAvatarUrl } from '../utils';
import { IncidentTableRow } from '../types';
import { getPlainTextDescriptionFromAssertion } from '../../Dataset/Validations/assertion/profile/summary/utils';
import { INCIDENT_STATE_TO_ACTIVITY, DEFAULT_MAX_ENTITIES_TO_SHOW } from './constant';
import { IncidentActivitySection } from './IncidentActivitySection';
import { getOnOpenAssertionLink } from '../hooks';

const ThinDivider = styled(Divider)`
    margin: 12px 0px;
    border-color: ${colors.gray[100]};
`;

const IncidentStates = {
    [IncidentState.Active]: {
        label: IncidentState.Active,
        icon: <Warning color={colors.red[1200]} width={20} height={20} />,
    },
    [IncidentState.Resolved]: {
        label: IncidentState.Resolved,
        icon: <Check color={colors.green[1200]} width={20} height={20} />,
    },
};

export const IncidentView = ({ incident }: { incident: IncidentTableRow }) => {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const [getAssigneeEntities, { data: resolvedAssignees, loading }] = useGetEntitiesLazyQuery();

    const [isDescriptionOpen, setDescriptionOpen] = useState<boolean>(true);
    const [incidentCreator, setIncidentCreator] = useState<CorpUser | any>();
    const [incidentResolver, setIncidentResolver] = useState<CorpUser | any>();

    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);

    const { type, source } = incident.source || {};
    const { label, icon } = IncidentStates[incident?.state] || {};

    const incidentActivityActors = {
        creator: incident?.creator?.actor,
        lastUpdated: incident?.lastUpdated?.actor,
    };

    useEffect(() => {
        // Dynamically construct the urns array
        const urns: any = [];

        if (incidentActivityActors.creator) urns.push(incidentActivityActors?.creator);
        if (incidentActivityActors.lastUpdated) urns.push(incidentActivityActors?.lastUpdated);

        if (urns?.length) {
            getAssigneeEntities({
                variables: {
                    urns,
                },
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [incident?.creator?.actor, incident?.lastUpdated?.actor]);

    useEffect(() => {
        if (resolvedAssignees?.entities?.length) {
            resolvedAssignees.entities.forEach((entity) => {
                if (incidentActivityActors.creator === incidentActivityActors.lastUpdated) {
                    setIncidentCreator(entity);
                    setIncidentResolver(entity);
                } else if (entity?.urn === incidentActivityActors.creator) {
                    setIncidentCreator(entity);
                } else if (entity?.urn === incidentActivityActors.lastUpdated) {
                    setIncidentResolver(entity);
                }
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [resolvedAssignees]);

    const navigateToUser = (user) => {
        history.push(`${entityRegistry.getEntityUrl(EntityType.CorpUser, user.urn)}`);
    };

    const renderAvatar = (assignee) => {
        return (
            <Avatar
                name={assignee?.name}
                imageUrl={assignee?.imageUrl}
                showInPill
                onClick={() => navigateToUser(assignee)}
            />
        );
    };

    const renderAssignees = (assignees) => {
        return assignees?.map((assignee) => {
            return <ListItemContainer key={assignee?.name}>{renderAvatar(assignee)}</ListItemContainer>;
        });
    };

    const renderActivities = (() => {
        // TODO Amit: Move this into a separate utilities file.
        const { creator, lastUpdated } = incidentActivityActors;
        const isCreatedAndUpdatedBySameUser = creator === lastUpdated;

        if (isCreatedAndUpdatedBySameUser) {
            if (incident?.state === IncidentState.Resolved) {
                return [
                    {
                        actor: incidentCreator,
                        action: INCIDENT_STATE_TO_ACTIVITY.RAISED,
                        time: incident?.creator?.time,
                    },
                    {
                        actor: incidentResolver,
                        action: INCIDENT_STATE_TO_ACTIVITY.RESOLVED,
                        time: incident?.lastUpdated?.time,
                    },
                ];
            }

            if (incident?.state === IncidentState.Active) {
                return [
                    {
                        actor: incidentCreator,
                        action: INCIDENT_STATE_TO_ACTIVITY.RAISED,
                        time: incident?.creator?.time,
                    },
                ];
            }
        }

        return [
            {
                actor: incidentCreator,
                action: INCIDENT_STATE_TO_ACTIVITY.RAISED,
                time: incident?.creator?.time,
            },
            {
                actor: incidentResolver,
                action: INCIDENT_STATE_TO_ACTIVITY.RESOLVED,
                time: incident?.lastUpdated?.time,
            },
        ];
    })();

    /** Assertion Related Logic. */
    const isAssertionFailureIncident = type === IncidentSourceType.AssertionFailure;
    const assertion = source as Assertion;
    const onClickAssertion = getOnOpenAssertionLink(assertion?.urn);

    let assertionDescription = '';
    if (isAssertionFailureIncident && source) {
        assertionDescription = getPlainTextDescriptionFromAssertion(assertion.info as AssertionInfo);
    }

    return (
        <Container>
            <DescriptionSection>
                <Header onClick={() => setDescriptionOpen(!isDescriptionOpen)}>
                    <DetailsLabel>Description</DetailsLabel>
                    <CompactMarkdownViewer
                        content={incident?.description || ''}
                        lineLimit={2}
                        fixedLineHeight
                        scrollableY={false}
                    />
                </Header>
            </DescriptionSection>
            <DetailsSection>
                <DetailsLabel>Category</DetailsLabel>
                <CategoryText>{incident?.type && getCapitalizeWord(incident?.type)}</CategoryText>
            </DetailsSection>
            <DetailsSection>
                <DetailsLabel>Priority</DetailsLabel>
                <IncidentPriorityLabel
                    priority={incident?.priority}
                    title={incident?.priority ? getCapitalizeWord(incident?.priority) : incident?.priority}
                />
            </DetailsSection>
            <DetailsSection>
                <DetailsLabel>Stage</DetailsLabel>
                <IncidentStagePill stage={incident?.stage} />
            </DetailsSection>
            <DetailsSection>
                <DetailsLabel>Assignees</DetailsLabel>
                <ListContainer>{renderAssignees(getAssigneeNamesWithAvatarUrl(incident?.assignees))}</ListContainer>
            </DetailsSection>
            <DetailsSection>
                <DetailsLabel>Linked Assets</DetailsLabel>
                <ListContainer style={{ maxWidth: '30vw' }}>
                    <EntityLinkList
                        entities={incident?.linkedAssets?.slice(0, entityCount)}
                        showMore={incident?.linkedAssets?.length > entityCount}
                        loading={false}
                        showMoreCount={
                            entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW > incident?.linkedAssets?.length
                                ? incident?.linkedAssets?.length - entityCount
                                : DEFAULT_MAX_ENTITIES_TO_SHOW
                        }
                        onClickMore={() => setEntityCount(entityCount + DEFAULT_MAX_ENTITIES_TO_SHOW)}
                    />
                </ListContainer>
            </DetailsSection>
            <DetailsSection>
                <DetailsLabel>State</DetailsLabel>
                <CategoryText>
                    <IconLabel
                        style={{ paddingLeft: 8 }}
                        name={getCapitalizeWord(label)}
                        icon={icon}
                        type={IconType.ICON}
                    />
                </CategoryText>
            </DetailsSection>

            {isAssertionFailureIncident ? (
                <DetailsSection>
                    <DetailsLabel>Raised By</DetailsLabel>
                    <Text onClick={() => onClickAssertion()}>{assertionDescription}</Text>
                </DetailsSection>
            ) : null}
            <ThinDivider />
            <IncidentActivitySection loading={loading} renderActivities={renderActivities} />
        </Container>
    );
};
