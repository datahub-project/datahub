import React from 'react';
import styled from 'styled-components';

import Module from '@app/homeV3/module/Module';

import { PageTemplateRowFragment } from '@graphql/template.generated';

const RowWrapper = styled.div`
    display: flex;
    gap: 16px;
    width: 100%;
    flex: 1;
`;

interface Props {
    row: PageTemplateRowFragment;
}

export default function TemplateRow({ row }: Props) {
    return (
        <RowWrapper>
            {row.modules.map((module) => (
                <Module key={module.urn} module={module} />
            ))}
        </RowWrapper>
    );
}
