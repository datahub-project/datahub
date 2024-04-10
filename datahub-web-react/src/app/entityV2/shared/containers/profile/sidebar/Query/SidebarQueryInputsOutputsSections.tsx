import React, { useContext } from 'react';
import styled from 'styled-components';
import { SidebarSection } from '../SidebarSection';
import EntitySidebarContext from '../../../../../../sharedV2/EntitySidebarContext';
import { useBaseEntity } from '../../../../../../entity/shared/EntityContext';
import { QueryEntity } from '../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../constants';

const HeaderColumn = styled.th`
    padding: 0 0.5em 0 0;
    text-align: left;
    vertical-align: top;
`;

const TextColumn = styled.td`
    text-wrap: wrap;
`;

const Container = styled.div`
    color: ${REDESIGN_COLORS.DARK_GREY};
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
                <Container>
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
                </Container>
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
                <Container>
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
                </Container>
            }
        />
    );
}
