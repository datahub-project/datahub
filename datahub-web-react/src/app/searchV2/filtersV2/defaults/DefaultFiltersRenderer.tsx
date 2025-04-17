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
