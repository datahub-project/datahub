import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { Entity } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getAutoCompleteEntityText } from './utils';
import ParentContainers from './ParentContainers';
import { ANTD_GRAY } from '../../entity/shared/constants';
import AutoCompleteEntityIcon from './AutoCompleteEntityIcon';
import { SuggestionText } from './styledComponents';

const AutoCompleteEntityWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
    align-items: center;
`;

const IconsContainer = styled.div`
    display: flex;
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

const ItemHeader = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 3px;
    gap: 4px;
`;

interface Props {
    query: string;
    entity: Entity;
    siblings?: Array<Entity>;
    hasParentTooltip: boolean;
}

export default function AutoCompleteEntity({ query, entity, siblings, hasParentTooltip }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const displayName = entityRegistry.getDisplayName(entity.type, entity);
    const { matchedText, unmatchedText } = getAutoCompleteEntityText(displayName, query);
    const subtype = genericEntityProps?.subTypes?.typeNames?.[0];
    const entities = siblings?.length ? siblings : [entity];

    const parentContainers =
        entities
            .map((ent) => {
                const genericEntityPropsForEnt = entityRegistry.getGenericEntityProperties(ent.type, ent);
                const entParentContainers = genericEntityPropsForEnt?.parentContainers?.containers || [];
                return [...entParentContainers].reverse();
            })
            .find((containers) => containers.length > 0) ?? [];

    return (
        <AutoCompleteEntityWrapper>
            <ContentWrapper>
                <SuggestionText>
                    <ItemHeader>
                        <IconsContainer>
                            {entities.map((ent) => (
                                <AutoCompleteEntityIcon key={ent.urn} entity={ent} />
                            ))}
                        </IconsContainer>
                        <ParentContainers parentContainers={parentContainers} />
                    </ItemHeader>
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
