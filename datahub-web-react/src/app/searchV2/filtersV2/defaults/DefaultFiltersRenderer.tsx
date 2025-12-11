/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { memo } from 'react';
import styled from 'styled-components';

import { FiltersRendererProps } from '@app/searchV2/filtersV2/types';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

export default memo(({ filters }: FiltersRendererProps) => {
    return (
        <Container>
            {filters.map((filter) => {
                const FilterComponent = filter.component;
                return <FilterComponent {...filter.props} key={filter.fieldName} />;
            })}
        </Container>
    );
});
