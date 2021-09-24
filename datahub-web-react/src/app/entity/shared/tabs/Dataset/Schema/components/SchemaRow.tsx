import React, { useContext } from 'react';
import styled from 'styled-components';
import { EntityType } from '../../../../../../../types.generated';
import CompactContext from '../../../../../../shared/CompactContext';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { FkContext } from '../utils/selectedFkContext';

const ForeignKeyContent = styled.div`
    position: relative;
    border: 1px solid red;
    min-height: 600px;
    width: 100%;
    display: table-row;
`;

const EntitySidePanel = styled.div`
    width: 300px;
`;

export const SchemaRow = ({
    children,
    className,
    'data-row-key': fieldPath,
}: {
    children: any;
    className: string;
    'data-row-key': string;
}) => {
    const selectedFk = useContext(FkContext);
    const entityRegistry = useEntityRegistry();

    return (
        <>
            <tr className={className}>{children}</tr>
            {fieldPath === selectedFk?.fieldPath && (
                <ForeignKeyContent>
                    <EntitySidePanel>
                        <CompactContext.Provider value>
                            {entityRegistry.renderProfile(
                                EntityType.Dataset,
                                selectedFk.constraint?.foreignDataset?.urn || '',
                            )}
                        </CompactContext.Provider>
                    </EntitySidePanel>
                </ForeignKeyContent>
            )}
        </>
    );
};
