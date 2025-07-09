import React from 'react';
import styled from 'styled-components';

import Module from '@app/homeV3/module/Module';
import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';

import { PageTemplateRowFragment } from '@graphql/template.generated';

const RowWrapper = styled.div`
    display: flex;
    gap: 16px;
    width: 100%;
    flex: 1;
`;

interface Props {
    row: PageTemplateRowFragment;
    modulesAvailableToAdd: ModulesAvailableToAdd;
    rowIndex: number;
}

export default function TemplateRow({ row, modulesAvailableToAdd, rowIndex }: Props) {
    return (
        <RowWrapper>
            <AddModuleButton
                orientation="vertical"
                modulesAvailableToAdd={modulesAvailableToAdd}
                rowIndex={rowIndex}
                rowSide="left"
            />

            {row.modules.map((module) => (
                <Module key={module.urn} module={module} />
            ))}

            <AddModuleButton
                orientation="vertical"
                modulesAvailableToAdd={modulesAvailableToAdd}
                rowIndex={rowIndex}
                rowSide="right"
            />
        </RowWrapper>
    );
}
