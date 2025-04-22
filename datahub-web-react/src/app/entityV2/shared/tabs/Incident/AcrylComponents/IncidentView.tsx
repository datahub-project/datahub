import { Check, Warning } from '@phosphor-icons/react';
import React, { memo, useEffect, useMemo, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { getPlainTextDescriptionFromAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/utils';
import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';
import { IncidentActivitySection } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentActivitySection';
import {
    DEFAULT_MAX_ENTITIES_TO_SHOW,
    INCIDENT_STATE_TO_ACTIVITY,
} from '@app/entityV2/shared/tabs/Incident/AcrylComponents/constant';
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
} from '@app/entityV2/shared/tabs/Incident/AcrylComponents/styledComponents';
import { getOnOpenAssertionLink } from '@app/entityV2/shared/tabs/Incident/hooks';
import { IncidentTableRow } from '@app/entityV2/shared/tabs/Incident/types';
import { getAssigneeNamesWithAvatarUrl } from '@app/entityV2/shared/tabs/Incident/utils';
import { Avatar } from '@src/alchemy-components';
import { IconLabel } from '@src/alchemy-components/components/IconLabel';
import { IconType } from '@src/alchemy-components/components/IconLabel/types';
import { IncidentPriorityLabel } from '@src/alchemy-components/components/IncidentPriorityLabel';
import { IncidentStagePill } from '@src/alchemy-components/components/IncidentStagePill';
import { getCapitalizeWord } from '@src/alchemy-components/components/IncidentStagePill/utils';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { EntityLinkList } from '@src/app/homeV2/reference/sections/EntityLinkList';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';
import {
    Assertion,
    AssertionInfo,
    EntityType,
    IncidentSourceType,
    IncidentState,
    IncidentType,
} from '@src/types.generated';

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

export const IncidentView = memo(({ incident }: { incident: IncidentTableRow }) => {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const [getAssigneeEntities, { data: resolvedAssignees, loading }] = useGetEntitiesLazyQuery();

    const [isDescriptionOpen, setIsDescriptionOpen] = useState<boolean>(true);

    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);

    const { type, source } = incident.source || {};
    const { label, icon } = IncidentStates[incident?.state] || {};

    const creatorUrn = useMemo(() => incident?.creator?.actor, [incident]);
    const lastUpdatedUrn = useMemo(() => incident?.lastUpdated?.actor, [incident]);

    useEffect(() => {
        const urns: string[] = [];

        if (creatorUrn) urns.push(creatorUrn);
        if (lastUpdatedUrn) urns.push(lastUpdatedUrn);

        if (urns.length) {
            getAssigneeEntities({ variables: { urns } });
        }
    }, [creatorUrn, lastUpdatedUrn, getAssigneeEntities]);

    const { incidentCreator, incidentResolver } = useMemo(() => {
        const entities = resolvedAssignees?.entities || [];

        if (creatorUrn === lastUpdatedUrn) {
            const commonEntity = entities.find((e) => e?.urn === creatorUrn);
            return { incidentCreator: commonEntity, incidentResolver: commonEntity };
        }

        const creator = entities.find((e) => e?.urn === creatorUrn);
        const resolver = entities.find((e) => e?.urn === lastUpdatedUrn);

        return { incidentCreator: creator, incidentResolver: resolver };
    }, [resolvedAssignees, creatorUrn, lastUpdatedUrn]);

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

    const renderActivities = useMemo(() => {
        const baseActivity = {
            actor: incidentCreator,
            action: INCIDENT_STATE_TO_ACTIVITY.RAISED,
            time: incident?.creator?.time,
        };

        if (incident?.state === IncidentState.Resolved) {
            return [
                baseActivity,
                {
                    actor: incidentResolver,
                    action: INCIDENT_STATE_TO_ACTIVITY.RESOLVED,
                    time: incident?.lastUpdated?.time,
                    message: incident?.message,
                },
            ];
        }

        return [baseActivity];
    }, [incident, incidentCreator, incidentResolver]);

    /** Assertion Related Logic. */
    const isAssertionFailureIncident = type === IncidentSourceType.AssertionFailure;
    const assertion = source as Assertion;
    const onClickAssertion = getOnOpenAssertionLink(assertion?.urn);

    let assertionDescription = '';
    if (isAssertionFailureIncident && source) {
        assertionDescription = getPlainTextDescriptionFromAssertion(assertion.info as AssertionInfo);
    }

    const categoryName = getCapitalizeWord(
        incident?.type === IncidentType.Custom ? incident.customType : incident.type,
    );

    return (
        <Container>
            <DescriptionSection>
                <Header onClick={() => setIsDescriptionOpen(!isDescriptionOpen)}>
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
                <CategoryText>{categoryName}</CategoryText>
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
});
