import StructuredPropertyValue from '@src/app/entityV2/shared/tabs/Properties/StructuredPropertyValue';
import { mapStructuredPropertyValues } from '@src/app/entityV2/shared/tabs/Properties/useStructuredProperties';
import { Popover, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { colors } from '@src/alchemy-components';
import { ActionRequest, ActionRequestOrigin, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import AiActorLabel from './AiActorLabel';
import CreatedByView from './CreatedByView';
import MetadataAssociationRequestItem from './MetadataAssociationRequestItem';
import RequestTargetEntityView from './RequestTargetEntityView';

const ContentWrapper = styled.span`
    font-size: 14px;
    // align all items in the center vertically
    align-items: center;
`;

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

const ValueContainer = styled.div`
    overflow: hidden;
    display: flex;
    max-width: 200px;
    border: 1px solid ${colors.gray[100]};
    border-radius: 200px;
    padding: 0px 8px;
`;

const AndOthersText = styled.div`
    padding-left: 6px;
    padding-right: 0px;
    padding-bottom: 2px;
    padding-top: 0px;
    color: ${colors.violet[400]};
    font-size: 14px;
    :hover {
        color: ${colors.violet[200]};
    }
`;

const BoldText = styled.span`
    font-weight: 700;
`;

const ValuesPopoverTitle = styled(BoldText)`
    font-size: 14px;
`;

type Props = {
    actionRequest: ActionRequest;
    showActionsButtons: boolean;
    onUpdate: () => void;
};

const REQUEST_TYPE_DISPLAY_NAME = 'Structured Property Proposal';

/**
 * A list item representing a structured property association request.
 */
export default function StructuredPropertyAsssociationRequestItem({
    actionRequest,
    showActionsButtons,
    onUpdate,
}: Props) {
    const entityRegistry = useEntityRegistry();

    // Extract structured property from the action request params
    const property = actionRequest.params?.structuredPropertyProposal?.structuredProperties?.[0]?.structuredProperty;

    // Get the display name for the property using the entity registry
    if (!property) {
        return null;
    }

    const propertyName = entityRegistry.getDisplayName(EntityType.StructuredProperty, property);
    const proposedPropertyValues =
        actionRequest.params?.structuredPropertyProposal?.structuredProperties.flatMap((p) => {
            return mapStructuredPropertyValues(p);
        }) || [];

    // Function to truncate the list of values to 1
    const getTruncatedValues = (values) => {
        if (values.length <= 1) {
            return { firstValue: values?.[0], remainingCount: 0 };
        }
        return { firstValue: values?.[0], remainingCount: values.length - 1 };
    };

    const { firstValue, remainingCount } = getTruncatedValues(proposedPropertyValues);

    const contentView = (
        <ContentWrapper>
            {origin === ActionRequestOrigin.Inferred ? (
                <AiActorLabel />
            ) : (
                <CreatedByView actionRequest={actionRequest} />
            )}
            <Typography.Text> requests to update property </Typography.Text>
            <BoldText>{`${propertyName}`}</BoldText>
            <Typography.Text>{` to `}</Typography.Text>
            {/* add the value of the property */}
            <ValuesContainer>
                <ValuesContainerFlex>
                    {firstValue && (
                        <ValueContainer>
                            <StructuredPropertyValue value={firstValue} size={14} truncateText />
                        </ValueContainer>
                    )}
                    {remainingCount > 0 && (
                        <Popover
                            content={
                                <ValuesContainerFlex>
                                    {proposedPropertyValues.map((value) => (
                                        <ValueContainer style={{ margin: 4 }}>
                                            <StructuredPropertyValue value={value} size={14} />
                                        </ValueContainer>
                                    ))}
                                </ValuesContainerFlex>
                            }
                            title={<ValuesPopoverTitle>Proposed Values</ValuesPopoverTitle>}
                            showArrow={false}
                        >
                            <AndOthersText>
                                + {remainingCount} other{remainingCount > 1 ? 's' : null}
                            </AndOthersText>
                        </Popover>
                    )}
                </ValuesContainerFlex>
            </ValuesContainer>
            <Typography.Text>{` on `}</Typography.Text>
            <RequestTargetEntityView actionRequest={actionRequest} />
        </ContentWrapper>
    );

    return (
        <MetadataAssociationRequestItem
            requestTypeDisplayName={REQUEST_TYPE_DISPLAY_NAME}
            requestContentView={contentView}
            actionRequest={actionRequest}
            onUpdate={onUpdate}
            showActionsButtons={showActionsButtons}
        />
    );
}
