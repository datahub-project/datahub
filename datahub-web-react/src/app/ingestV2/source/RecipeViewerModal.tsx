import { Modal } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { CodeBlock } from '@components/components/CodeBlock';

import { jsonToYaml } from '@app/ingestV2/source/utils';

const EDITOR_LANGUAGE = 'yaml';

interface Props {
    recipe?: string;
    onCancel: () => void;
}

function RecipeViewerModal({ recipe, onCancel }: Props) {
    const { t } = useTranslation('ingestion');
    const { t: tc } = useTranslation('common.actions');
    const formattedRecipe = recipe ? jsonToYaml(recipe) : '';

    return (
        <Modal
            onCancel={onCancel}
            width={800}
            title={t('source.viewRecipeTitle')}
            buttons={[{ text: tc('done'), variant: 'filled', onClick: onCancel }]}
        >
            <CodeBlock
                code={formattedRecipe}
                language={EDITOR_LANGUAGE}
                variant="embedded"
                showHeader={false}
                showCopy={false}
                showFormat={false}
                showLineNumbers
                maxHeight="55vh"
            />
        </Modal>
    );
}

export default RecipeViewerModal;
