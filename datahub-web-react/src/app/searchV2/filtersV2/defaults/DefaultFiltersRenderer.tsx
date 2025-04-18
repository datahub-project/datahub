import React, { memo } from 'react';
import styled from 'styled-components';
<<<<<<< HEAD

import { FiltersRendererProps } from '@app/searchV2/filtersV2/types';
=======
import { FiltersRendererProps } from '../types';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
