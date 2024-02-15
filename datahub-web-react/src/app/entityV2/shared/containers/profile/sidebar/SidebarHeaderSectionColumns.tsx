import React from 'react';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '../../../constants';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useEntityData } from '../../../EntityContext';
import CompactContext from '../../../../../shared/CompactContext';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 10px;
    border-bottom: 1px solid #0000001a;
    margin-left: -20px;
    margin-right: -20px;
`;

const ColumnsContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: start;
    justify-content: start;
    margin-left: 5px;

    & > div {
        &:not(:first-child) {
            border-left: 1px dashed;
            border-color: rgba(0, 0, 0, 0.3);
        }
    }
`;

const EntityName = styled.div`
    margin-left: 20px;
`;

const Column = styled.div`
    font-weight: bold;
    font-size: 12px;
    display: flex;
    flex-direction: column;
    align-items: space-between;
    justify-content: start;
    margin-right: 20px;
    padding-left: 20px;
`;

const Heading = styled.div`
    display: flex;
    gap: 4px;
    color: ${REDESIGN_COLORS.DARK_GREY};
    align-items: center;
    margin-bottom: 4px;
`;

const Title = styled.div`
    font-size: 12px;
    font-weight: 600;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

const EntityTitle = styled.div<{ showEntityName?: boolean }>`
    font-size: 16px;
    font-weight: 800;
    line-height: 20px;
    color: ${REDESIGN_COLORS.HEADING_COLOR};
    ${(props) =>
        props.showEntityName &&
        `
    :hover {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
    `}
`;

type SidebarStatsColumn = {
    title: React.ReactNode;
    content: React.ReactNode;
    icon: React.ReactNode;
};

type Props = {
    columns: SidebarStatsColumn[];
};

export const SidebarHeaderSectionColumns = ({ columns }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { urn, entityType, entityData } = useEntityData();

    const entityName = entityData ? entityRegistry.getDisplayName(entityType, entityData) : '';

    const isCompact = React.useContext(CompactContext);
    const showEntityName = isCompact;

    return (
        <Container className="top-section">
            {showEntityName && (
                <EntityName>
                    <a href={entityRegistry.getEntityUrl(entityType, urn)}>
                        <EntityTitle showEntityName={showEntityName}> {entityName}</EntityTitle>
                    </a>
                </EntityName>
            )}
            {columns.length > 0 && (
                <ColumnsContainer>
                    {columns.map((column) => (
                        <Column>
                            <Heading>
                                <Title>{column.title}</Title>
                            </Heading>
                            {column.content}
                        </Column>
                    ))}
                </ColumnsContainer>
            )}
        </Container>
    );
};
