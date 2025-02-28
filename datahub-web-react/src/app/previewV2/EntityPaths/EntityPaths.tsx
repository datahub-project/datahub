import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { EntityPath } from '../../../types.generated';
import ColumnPathsText from './ColumnPathsText';
import EntityPathsModal from './EntityPathsModal';

const EntityPathsWrapper = styled.div`
    padding-bottom: 10px;
`;

interface Props {
    paths: EntityPath[];
    resultEntityUrn: string;
}

export default function EntityPaths({ paths, resultEntityUrn }: Props) {
    const [isPathsModalVisible, setIsPathsModalVisible] = useState(false);

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
