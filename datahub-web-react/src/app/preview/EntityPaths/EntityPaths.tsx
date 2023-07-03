import React, { useContext, useState } from 'react';
import styled from 'styled-components/macro';
import { EntityPath } from '../../../types.generated';
import { LineageTabContext } from '../../entity/shared/tabs/Lineage/LineageTabContext';
import ColumnPathsText from './ColumnPathsText';
import EntityPathsModal from './EntityPathsModal';

const EntityPathsWrapper = styled.div`
    margin-bottom: 5px;
`;

interface Props {
    paths: EntityPath[];
    resultEntityUrn: string;
}

export default function EntityPaths({ paths, resultEntityUrn }: Props) {
    const { isColumnLevelLineage, selectedColumn } = useContext(LineageTabContext);
    const [isPathsModalVisible, setIsPathsModalVisible] = useState(false);

    if (!isColumnLevelLineage || !selectedColumn) return null;

    return (
        <>
            <EntityPathsWrapper>
                <ColumnPathsText
                    paths={paths}
                    resultEntityUrn={resultEntityUrn}
                    openModal={() => setIsPathsModalVisible(true)}
                />
            </EntityPathsWrapper>
            {isPathsModalVisible && (
                <EntityPathsModal
                    paths={paths}
                    resultEntityUrn={resultEntityUrn}
                    hideModal={() => setIsPathsModalVisible(false)}
                />
            )}
        </>
    );
}
