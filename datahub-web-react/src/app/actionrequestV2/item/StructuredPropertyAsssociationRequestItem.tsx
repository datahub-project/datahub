import { Text } from '@components';
import { colors } from '@src/alchemy-components';
import StructuredPropertyValue from '@src/app/entityV2/shared/tabs/Properties/StructuredPropertyValue';
import { mapStructuredPropertyValues } from '@src/app/entityV2/shared/tabs/Properties/useStructuredProperties';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { ActionRequest, ActionRequestOrigin, ActionRequestResult, EntityType, StdDataType } from '@src/types.generated';
import { Popover } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useHydratedEntityMap } from '@src/app/entityV2/shared/tabs/Properties/useHydratedEntityMap';
import { pluralize } from '@src/app/shared/textUtil';
import AiActorLabel from './AiActorLabel';
import CreatedByView from './CreatedByView';
import RequestTargetEntityView from './RequestTargetEntityView';
import { ContentWrapper } from './styledComponents';

const ValuesContainer = styled.div`
    display: inline-block;
    vertical-align: middle;
`;

const ValuesContainerFlex = styled.div`
    display: flex;
    font-size: 14px;
    // align all items in the center vertically
    align-items: center;
`;

const ValueContainer = styled.div<{ $showBorder?: boolean }>`
    overflow: hidden;
    display: flex;
    max-width: 200px;
    border-radius: 200px;
    padding: 0px 4px;
    ${(props) =>
        props.$showBorder &&
        `
        border: 1px solid ${colors.gray[200]};
        `}
`;

const AndOthersText = styled(Text)`
    color: ${colors.violet[400]};
    :hover {
        color: ${colors.violet[200]};
    }
`;

interface Props {
    actionRequest: ActionRequest;
}

const StructuredPropertyAssociationRequestItem = ({ actionRequest }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    // Extract structured properties from the action request params
    const properties = actionRequest.params?.structuredPropertyProposal?.structuredProperties?.flatMap(
        (prop) => prop.structuredProperty,
    );

    const allValues =
        actionRequest.params?.structuredPropertyProposal?.structuredProperties.flatMap((p) => {
            return mapStructuredPropertyValues(p);
        }) || [];

    const hydratedEntityMap = useHydratedEntityMap(allValues.map((val) => val.entity?.urn));

    if (!properties) {
        return null;
    }

    // Function to truncate the list of values to 1
    const getTruncatedValues = (values) => {
        if (values.length <= 1) {
            return { firstValue: values?.[0], remainingCount: 0 };
        }
        return { firstValue: values?.[0], remainingCount: values.length - 1 };
    };

    const isApproved = actionRequest.result === ActionRequestResult.Accepted;

    return (
        <ContentWrapper>
            {origin === ActionRequestOrigin.Inferred ? (
                <AiActorLabel />
            ) : (
                <CreatedByView actionRequest={actionRequest} />
            )}
            <Text color="gray" weight="medium" type="span">
                {' '}
                requests to update {pluralize(properties.length, 'property')}
            </Text>
            {properties.map((property, index) => {
                // Get the display name for the property using the entity registry
                const propertyName = entityRegistry.getDisplayName(EntityType.StructuredProperty, property);
                const isRichText = property.definition.valueType.info.type === StdDataType.RichText;

                const proposedPropertyValues =
                    actionRequest.params?.structuredPropertyProposal?.structuredProperties
                        .filter((p) => p.structuredProperty.urn === property.urn)
                        .flatMap((prop) => {
                            return mapStructuredPropertyValues(prop);
                        }) || [];

                const { firstValue, remainingCount } = getTruncatedValues(proposedPropertyValues);

                return (
                    <>
                        {index > 0 && index === properties.length - 1 && 'and'}
                        {index > 0 && index < properties.length - 1 && ','}
                        <Text weight="bold" color="gray" type="span">{`${propertyName}`}</Text>
                        {` to `}

                        {/* add the value of the property */}
                        <ValuesContainer>
                            <ValuesContainerFlex>
                                {firstValue && (
                                    <ValueContainer $showBorder={isApproved && !firstValue.entity}>
                                        <StructuredPropertyValue
                                            value={firstValue}
                                            size={14}
                                            truncateText
                                            isRichText={isRichText}
                                            isProposed={!isApproved}
                                            hydratedEntityMap={hydratedEntityMap}
                                        />
                                    </ValueContainer>
                                )}
                                {remainingCount > 0 && (
                                    <Popover
                                        content={
                                            <ValuesContainerFlex>
                                                {proposedPropertyValues.map((value) => (
                                                    <ValueContainer $showBorder={isApproved && !value.entity}>
                                                        <StructuredPropertyValue
                                                            value={value}
                                                            size={14}
                                                            isRichText={isRichText}
                                                            isProposed={!isApproved}
                                                            hydratedEntityMap={hydratedEntityMap}
                                                        />
                                                    </ValueContainer>
                                                ))}
                                            </ValuesContainerFlex>
                                        }
                                        title={
                                            <Text color="gray" weight="bold">
                                                Proposed Values
                                            </Text>
                                        }
                                        showArrow={false}
                                    >
                                        <AndOthersText weight="medium">
                                            + {remainingCount} other{remainingCount > 1 ? 's' : null}
                                        </AndOthersText>
                                    </Popover>
                                )}
                            </ValuesContainerFlex>
                        </ValuesContainer>
                    </>
                );
            })}
            <Text color="gray" weight="medium" type="span">{` on `}</Text>
            <RequestTargetEntityView actionRequest={actionRequest} />
        </ContentWrapper>
    );
};

export default StructuredPropertyAssociationRequestItem;
