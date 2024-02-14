import React, { useContext } from 'react';
import styled from 'styled-components';
import { SidebarSection } from '../SidebarSection';
import EntitySidebarContext from '../../../../../../shared/EntitySidebarContext';
import { useBaseEntity } from '../../../../EntityContext';
import { QueryEntity } from '../../../../../../../types.generated';

const HeaderColumn = styled.th`
    padding: 0 0.5em 0 0;
    text-align: left;
    vertical-align: top;
`;

const TextColumn = styled.td`
    text-wrap: wrap;
`;

export function SidebarQueryInputsSection() {
    const { extra } = useContext(EntitySidebarContext);
    const query = useBaseEntity<{ entity: QueryEntity | null }>()?.entity;

    if (!query) {
        return null;
    }

    return (
        <SidebarSection
            title="Inputs"
            content={
                <>
                    <table cellSpacing={0} cellPadding={0}>
                        <tbody>
                            <tr>
                                <HeaderColumn>Tables:</HeaderColumn>
                                <TextColumn>{extra?.inputTables.join(', ')}</TextColumn>
                            </tr>
                            {extra?.inputColumns && (
                                <tr>
                                    <HeaderColumn>Columns:</HeaderColumn>
                                    <TextColumn>
                                        {extra?.inputColumns.map(([table, col]) => `${table}.${col}`).join(', ')}
                                    </TextColumn>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </>
            }
        />
    );
}

export function SidebarQueryOutputsSection() {
    const { extra } = useContext(EntitySidebarContext);
    const query = useBaseEntity<{ entity: QueryEntity | null }>()?.entity;

    if (!query) {
        return null;
    }

    return (
        <SidebarSection
            title="Outputs"
            content={
                <>
                    <table cellSpacing={0} cellPadding={0}>
                        <tbody>
                            <tr>
                                <HeaderColumn>Tables:</HeaderColumn>
                                <TextColumn>{extra?.outputTables.join(', ')}</TextColumn>
                            </tr>
                            {extra?.outputColumns && (
                                <tr>
                                    <HeaderColumn>Columns:</HeaderColumn>
                                    <TextColumn>
                                        {extra?.outputColumns.map(([table, col]) => `${table}.${col}`).join(', ')}
                                    </TextColumn>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </>
            }
        />
    );
}
