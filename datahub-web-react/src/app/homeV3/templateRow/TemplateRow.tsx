import React from 'react';
import styled from 'styled-components';

import Module from '@app/homeV3/module/Module';
import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import { AddModuleInput } from '@app/homeV3/template/types';

import { DataHubPageTemplateRow } from '@types';

const RowWrapper = styled.div`
    display: flex;
    gap: 16px;
    width: 100%;
    flex: 1;
`;

interface Props {
    row: DataHubPageTemplateRow;
    onAddModule?: (input: AddModuleInput) => void;
    modulesAvailableToAdd: ModulesAvailableToAdd;
    rowIndex: number;
}

export default function TemplateRow({ row, onAddModule, modulesAvailableToAdd, rowIndex }: Props) {
    return (
        <RowWrapper>
            <AddModuleButton
                orientation="vertical"
                modulesAvailableToAdd={modulesAvailableToAdd}
                onAddModule={onAddModule}
                rowIndex={rowIndex}
                rowSide="left"
            />

            {row.modules.map((module) => (
                <Module key={module.urn} module={module} />
            ))}

            <AddModuleButton
                orientation="vertical"
                modulesAvailableToAdd={modulesAvailableToAdd}
                onAddModule={onAddModule}
                rowIndex={rowIndex}
                rowSide="right"
            />
        </RowWrapper>
    );
}
