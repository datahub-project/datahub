/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import Editor from '@monaco-editor/react';
import { Button, Modal } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { jsonToYaml } from '@app/ingest/source/utils';

const YamlWrapper = styled.div`
    padding: 24px;
`;

interface Props {
    recipe?: string;
    onCancel: () => void;
}

function RecipeViewerModal({ recipe, onCancel }: Props) {
    const formattedRecipe = recipe ? jsonToYaml(recipe) : '';

    return (
        <Modal
            open
            onCancel={onCancel}
            width={800}
            title="View Ingestion Recipe"
            footer={<Button onClick={onCancel}>Done</Button>}
        >
            <YamlWrapper>
                <Editor
                    options={{
                        readOnly: true,
                        minimap: { enabled: false },
                        scrollbar: {
                            vertical: 'hidden',
                            horizontal: 'hidden',
                        },
                    }}
                    height="55vh"
                    defaultLanguage="yaml"
                    value={formattedRecipe}
                />
            </YamlWrapper>
        </Modal>
    );
}

export default RecipeViewerModal;
