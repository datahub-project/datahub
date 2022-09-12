import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useEffect, useState } from 'react';
import { set, get } from 'lodash';
import YAML from 'yamljs';
import styled from 'styled-components/macro';
import { Transformer } from '../types';
import TransformerInput from './TransformerInput';
import { jsonToYaml } from '../../utils';
import { useGetEntitiesLazyQuery } from '../../../../../graphql/entity.generated';
import { Entity } from '../../../../../types.generated';
import usePrevious from '../../../../shared/usePrevious';

const AddTransformerButton = styled(Button)`
    margin: 10px 0;
`;

function updateRecipe(displayRecipe: string, transformers: Transformer[], setStagedRecipe: (recipe: string) => void) {
    const jsonRecipe = YAML.parse(displayRecipe);
    const jsonTransformers = transformers
        .filter((t) => t.type)
        .map((transformer) => {
            return {
                type: transformer.type,
                config: {
                    urns: transformer.urns,
                },
            };
        });
    const transformersValue = jsonTransformers.length > 0 ? jsonTransformers : undefined;
    set(jsonRecipe, 'transformers', transformersValue);
    const stagedRecipe = jsonToYaml(JSON.stringify(jsonRecipe));
    setStagedRecipe(stagedRecipe);
}

function getInitialState(displayRecipe: string) {
    const jsonState = YAML.parse(displayRecipe);
    const jsonTransformers = get(jsonState, 'transformers') || [];
    return jsonTransformers.map((t) => {
        return { type: t.type, urns: t.config.urns || [] };
    });
}

interface Props {
    displayRecipe: string;
    setStagedRecipe: (recipe: string) => void;
}

export default function AddTransformers({ displayRecipe, setStagedRecipe }: Props) {
    const [getEntities, { data, loading }] = useGetEntitiesLazyQuery();
    const [transformers, setTransformers] = useState<Transformer[]>(getInitialState(displayRecipe));

    useEffect(() => {
        updateRecipe(displayRecipe, transformers, setStagedRecipe);
    }, [transformers, displayRecipe, setStagedRecipe]);

    function addNewTransformer() {
        setTransformers((prevTransformers) => [...prevTransformers, { type: null, urns: [] }]);
    }

    const urns = transformers.map((t) => t.urns).flat();
    const previousUrnsLength = usePrevious(urns.length) || 0;
    useEffect(() => {
        if (urns.length && !loading && (!data || urns.length > previousUrnsLength)) {
            getEntities({ variables: { urns } });
        }
    }, [urns, getEntities, data, loading, previousUrnsLength]);

    const existingTransformerTypes = transformers.map((transformer) => transformer.type);

    return (
        <>
            {transformers.map((transformer, index) => {
                const key = `${transformer.type}-${index}`;
                return (
                    <TransformerInput
                        key={key}
                        transformer={transformer}
                        existingTransformerTypes={existingTransformerTypes}
                        index={index}
                        entities={data?.entities as Entity[]}
                        setTransformers={setTransformers}
                    />
                );
            })}
            <AddTransformerButton onClick={addNewTransformer}>
                <PlusOutlined />
                Add Transformer
            </AddTransformerButton>
        </>
    );
}
