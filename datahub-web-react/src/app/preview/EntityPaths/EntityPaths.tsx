/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useContext, useState } from 'react';
import styled from 'styled-components/macro';

import { LineageTabContext } from '@app/entity/shared/tabs/Lineage/LineageTabContext';
import ColumnPathsText from '@app/preview/EntityPaths/ColumnPathsText';
import EntityPathsModal from '@app/preview/EntityPaths/EntityPathsModal';

import { EntityPath } from '@types';

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
