/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import AttributeContent from '@app/shared/businessAttribute/AttributeContent';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { BusinessAttributeAssociation, EntityType } from '@types';

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
