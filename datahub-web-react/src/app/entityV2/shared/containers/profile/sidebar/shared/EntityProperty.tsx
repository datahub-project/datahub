/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { LabelText } from '@app/entityV2/shared/containers/profile/sidebar/shared/styledComponents';
import { EntityLink } from '@src/app/homeV2/reference/sections/EntityLink';
import { Entity } from '@src/types.generated';

const PropertyContainer = styled.div`
    display: flex;
    gap: 5px;
    margin-bottom: 6px;
    align-items: center; /* This will vertically center all items by default */

    // when this overflows, the entity link components should truncate text
    white-space: nowrap;
`;

interface Props {
    labelText: string;
    entity?: Entity | null;
}

const EntityProperty = ({ labelText, entity }: Props) => {
    return (
        <PropertyContainer>
            <LabelText>{labelText}</LabelText>
            {!!entity && <EntityLink entity={entity} />}
        </PropertyContainer>
    );
};

export default EntityProperty;
