import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { Transformer } from '../types';
import TransformerInput from './TransformerInput';
import { useGetEntitiesLazyQuery } from '../../../../../graphql/entity.generated';
import { Entity } from '../../../../../types.generated';
import usePrevious from '../../../../shared/usePrevious';
import { getInitialState, getUpdatedRecipe } from './utils';

const AddTransformerButton = styled(Button)`
    margin: 10px 0;
`;

interface Props {
    displayRecipe: string;
    setStagedRecipe: (recipe: string) => void;
}

export default function AddTransformers({ displayRecipe, setStagedRecipe }: Props) {
    const [getEntities, { data, loading }] = useGetEntitiesLazyQuery();
    const [transformers, setTransformers] = useState<Transformer[]>(getInitialState(displayRecipe));

    useEffect(() => {
        const updatedRecipe = getUpdatedRecipe(displayRecipe, transformers);
        setStagedRecipe(updatedRecipe);
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
