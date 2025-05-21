import React, { useState } from 'react';
import styled from 'styled-components/macro';

import ColumnPathsText from '@app/previewV2/EntityPaths/ColumnPathsText';
import EntityPathsModal from '@app/previewV2/EntityPaths/EntityPathsModal';

import { EntityPath } from '@types';

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
