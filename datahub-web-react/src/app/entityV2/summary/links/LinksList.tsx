import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import LinkItem from '@app/entityV2/summary/links/LinkItem';

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

export default function LinksList() {
    const { entityData } = useEntityData();
    const links = entityData?.institutionalMemory?.elements || [];

    if (links.length === 0) {
        return null;
    }

    return (
        <ListContainer>
            {links.map((link) => {
                return <LinkItem link={link} />;
            })}
        </ListContainer>
    );
}
