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
    gap: 8px;
`;

const PlatformText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 700;
    color: ${ANTD_GRAY[7]};
    white-space: nowrap;
`;

const PlatformDivider = styled.div`
    border-right: 1px solid ${ANTD_GRAY[5]};
    height: 14px;
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

    const platformNames = entities
        .map((ent) => {
            const genericPropsForEnt = entityRegistry.getGenericEntityProperties(ent.type, ent);
            const platformName = genericPropsForEnt?.platform?.name;
            return platformName;
        })
        .filter(Boolean);

    console.log({ entity: entity.urn, platformNames });

    const parentContainers =
        entities
            .map((ent) => {
                const genericPropsForEnt = entityRegistry.getGenericEntityProperties(ent.type, ent);
                const entParentContainers = genericPropsForEnt?.parentContainers?.containers || [];
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
                        {platformNames.length > 0 && (
                            <>
                                <PlatformText>{platformNames.join(' & ')}</PlatformText>
                                {!!parentContainers.length && <PlatformDivider />}
                            </>
                        )}
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
