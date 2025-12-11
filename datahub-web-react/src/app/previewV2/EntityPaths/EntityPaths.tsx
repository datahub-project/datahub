/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
