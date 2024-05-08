import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { BusinessAttributeAssociation, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { HoverEntityTooltip } from '../../recommendations/renderer/component/HoverEntityTooltip';
import AttributeContent from './AttributeContent';

const AttributeLink = styled(Link)`
    display: inline-block;
    margin-bottom: 8px;
`;

const AttributeWrapper = styled.span`
    display: inline-block;
    margin-bottom: 8px;
`;

interface Props {
    businessAttribute: BusinessAttributeAssociation;
    entityUrn?: string;
    entitySubresource?: string;
    canRemove?: boolean;
    readOnly?: boolean;
    highlightText?: string;
    fontSize?: number;
    onOpenModal?: () => void;
    refetch?: () => Promise<any>;
}

export default function StyledAttribute(props: Props) {
    const { businessAttribute, readOnly } = props;
    const entityRegistry = useEntityRegistry();

    if (readOnly) {
        return (
            <HoverEntityTooltip entity={businessAttribute?.businessAttribute}>
                <AttributeWrapper>
                    <AttributeContent {...props} />
                </AttributeWrapper>
            </HoverEntityTooltip>
        );
    }

    return (
        <HoverEntityTooltip entity={businessAttribute?.businessAttribute}>
            <AttributeLink
                to={entityRegistry.getEntityUrl(
                    EntityType.BusinessAttribute,
                    businessAttribute?.businessAttribute?.urn,
                )}
                key={businessAttribute?.businessAttribute?.urn}
            >
                <AttributeContent {...props} />
            </AttributeLink>
        </HoverEntityTooltip>
    );
}
