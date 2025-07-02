import React from 'react';
import styled, { useTheme } from 'styled-components';

import { getColor } from '@components/theme/utils';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import DisplayName from '@app/searchV2/autoCompleteV2/components/DisplayName';
import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import Matches from '@app/searchV2/autoCompleteV2/components/matches/Matches';
import EntitySubtitle from '@app/searchV2/autoCompleteV2/components/subtitle/EntitySubtitle';
import { VARIANT_STYLES } from '@app/searchV2/autoCompleteV2/constants';
import { EntityItemVariant } from '@app/searchV2/autoCompleteV2/types';
import { getEntityDisplayType } from '@app/searchV2/autoCompleteV2/utils';
import { Text } from '@src/alchemy-components';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity, MatchedField } from '@src/types.generated';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    padding: 8px 13px 8px 8px;

    :hover {
        cursor: pointer;
    }
`;

// FYI: this hovering dependent on Container can't be applied by condition inside of styled component
// so we have this separated version with hover
const DisplayNameWithHover = styled(DisplayName)<{ $decorationColor?: string }>`
    ${Container}:hover & {
        text-decoration: underline;
        ${(props) => props.$decorationColor && `text-decoration-color: ${props.$decorationColor};`}
    }
`;

const DisplayNameWrapper = styled.div`
    width: fit-content;
`;

const ContentContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 16px;
    overflow: hidden;
    width: 100%;
`;

const DescriptionContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: hidden;
    width: 100%;
`;

const IconContainer = styled.div<{ $variant?: EntityItemVariant }>`
    display: flex;
    align-items: ${(props) => {
        switch (props.$variant) {
            case 'searchBar':
                return 'flex-start';
            default:
                return 'center';
        }
    }};
    justify-content: center;
    width: 32px;
`;

const TypeContainer = styled.div`
    display: flex;
    align-items: center;
`;

interface EntityAutocompleteItemProps {
    entity: Entity;
    query?: string;
    siblings?: Entity[];
    matchedFields?: MatchedField[];
    variant?: EntityItemVariant;
}

export default function AutoCompleteEntityItem({
    entity,
    query,
    siblings,
    matchedFields,
    variant,
}: EntityAutocompleteItemProps) {
    const theme = useTheme();
    const entityRegistry = useEntityRegistryV2();
    const displayName = entityRegistry.getDisplayName(entity.type, entity);
    const displayType = getEntityDisplayType(entity, entityRegistry);
    const variantProps = VARIANT_STYLES.get(variant ?? 'default');

    return (
        <Container>
            <ContentContainer>
                <IconContainer $variant={variant}>
                    <EntityIcon entity={entity} siblings={siblings} />
                </IconContainer>

                <DescriptionContainer>
                    <HoverEntityTooltip
                        placement="bottom"
                        entity={entity}
                        showArrow={false}
                        canOpen={variantProps?.showEntityPopover}
                    >
                        <DisplayNameWrapper>
                            {variantProps?.nameCanBeHovered ? (
                                <DisplayNameWithHover
                                    displayName={displayName}
                                    highlight={query}
                                    color={variantProps?.nameColor}
                                    colorLevel={variantProps?.nameColorLevel}
                                    weight={variantProps?.nameWeight}
                                    $decorationColor={getColor(
                                        variantProps?.nameColor,
                                        variantProps?.nameColorLevel,
                                        theme,
                                    )}
                                />
                            ) : (
                                <DisplayName
                                    displayName={displayName}
                                    highlight={query}
                                    color={variantProps?.nameColor}
                                    colorLevel={variantProps?.nameColorLevel}
                                    weight={variantProps?.nameWeight}
                                />
                            )}
                        </DisplayNameWrapper>
                    </HoverEntityTooltip>

                    <EntitySubtitle
                        entity={entity}
                        color={variantProps?.subtitleColor}
                        colorLevel={variantProps?.subtitleColorLevel}
                    />

                    <Matches
                        matchedFields={matchedFields}
                        entity={entity}
                        query={query}
                        displayName={displayName}
                        color={variantProps?.matchColor}
                        colorLevel={variantProps?.matchColorLevel}
                    />
                </DescriptionContainer>
            </ContentContainer>

            <TypeContainer>
                <Text color={variantProps?.typeColor} colorLevel={variantProps?.typeColorLevel} size="sm">
                    {displayType}
                </Text>
            </TypeContainer>
        </Container>
    );
}
