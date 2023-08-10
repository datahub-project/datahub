import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { Entity } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getAutoCompleteEntityText } from './utils';
import { SuggestionText } from './AutoCompleteUser';
import ParentContainers from './ParentContainers';
import { ANTD_GRAY } from '../../entity/shared/constants';
import AutoCompleteEntityIcon from './AutoCompleteEntityIcon';

const AutoCompleteEntityWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
    align-items: center;
`;

const IconsContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const ContentWrapper = styled.div`
    display: flex;
    align-items: center;
    overflow: hidden;
`;

const Subtype = styled.span`
    color: ${ANTD_GRAY[9]};
    border: 1px solid ${ANTD_GRAY[9]};
    border-radius: 16px;
    padding: 4px 8px;
    line-height: 12px;
    font-size: 12px;
    margin-right: 8px;
`;

interface Props {
    query: string;
    entity: Entity;
    siblings?: Array<Entity>;
    hasParentTooltip: boolean;
}

// todo - this is mostly working well, but we really need to grab the entity details (path etc) from the bigquery one
// check out the search results de-dupe and see if there's something going on here that lets it pick the bq stuff

// todo - maybe for some reason the dbt being the primary is throwing things off here?

// todo - look at how Chris did the search result stuff, maybe we have some extra de-dupe step somewhere?
export default function AutoCompleteEntity({ query, entity, siblings, hasParentTooltip }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const displayName = entityRegistry.getDisplayName(entity.type, entity);

    const iconScale = siblings?.length ? 0.66 : 1;

    const { matchedText, unmatchedText } = getAutoCompleteEntityText(displayName, query);
    const parentContainers = genericEntityProps?.parentContainers?.containers || [];
    // Need to reverse parentContainers since it returns direct parent first.
    const orderedParentContainers = [...parentContainers].reverse();
    const subtype = genericEntityProps?.subTypes?.typeNames?.[0];

    const entitiesForIconRendering = [entity, ...(siblings ?? [])];

    return (
        <AutoCompleteEntityWrapper>
            <ContentWrapper>
                <IconsContainer>
                    {entitiesForIconRendering.map((renderableIcon) => (
                        <AutoCompleteEntityIcon key={renderableIcon.urn} entity={renderableIcon} scale={iconScale} />
                    ))}
                </IconsContainer>
                <SuggestionText>
                    <ParentContainers parentContainers={orderedParentContainers} />
                    <Typography.Text
                        ellipsis={
                            hasParentTooltip ? {} : { tooltip: { title: displayName, color: 'rgba(0, 0, 0, 0.9)' } }
                        }
                    >
                        <Typography.Text strong>{matchedText}</Typography.Text>
                        {unmatchedText}
                    </Typography.Text>
                </SuggestionText>
            </ContentWrapper>
            {subtype && <Subtype>{subtype.toLocaleLowerCase()}</Subtype>}
        </AutoCompleteEntityWrapper>
    );
}
