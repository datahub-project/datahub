import { DeleteOutlined } from '@ant-design/icons';
import { Button, Select } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { EntityType } from '../../../../../types.generated';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { EntitySearchInput } from '../../../../entity/shared/EntitySearchInput/EntitySearchInput';
import { Transformer, TransformerTypes } from '../types';

const TransformerWrapper = styled.div`
    // border-bottom: 1px solid ${ANTD_GRAY[4]};
    // border-top: 1px solid ${ANTD_GRAY[4]};
    align-items: flex-end;
    display: flex;
    padding: 12px 24px;
    margin: 0 -24px;

    &:hover {
        background-color: ${ANTD_GRAY[2]};
    }
`;

const InputWrapper = styled.div`
    flex: 1;
`;

const StyledSelect = styled(Select)`
    min-width: 150px;
`;

const EntitySearchWrapper = styled.div`
    margin-top: 10px;

    .ant-select {
        width: 100%;
    }
`;

const DeleteButton = styled(Button)`
    margin-left: 10px;
`;

function getPlaceholderText(type: string | null) {
    let entityName = 'entities';
    if (type === TransformerTypes.ADD_OWNERS) entityName = 'owners';
    if (type === TransformerTypes.ADD_TAGS) entityName = 'tags';
    if (type === TransformerTypes.ADD_TERMS) entityName = 'glossary terms';
    if (type === TransformerTypes.ADD_DOMAIN) entityName = 'domains';
    return `Search for ${entityName}...`;
}

function getEntityTypes(type: string | null) {
    switch (type) {
        case TransformerTypes.ADD_OWNERS:
            return [EntityType.CorpUser, EntityType.CorpGroup];
        case TransformerTypes.ADD_TAGS:
            return [EntityType.Tag];
        case TransformerTypes.ADD_TERMS:
            return [EntityType.GlossaryTerm];
        case TransformerTypes.ADD_DOMAIN:
            return [EntityType.Domain];
        default:
            return [];
    }
}

const typeOptions = [
    { value: TransformerTypes.ADD_OWNERS, label: 'Add Owners' },
    { value: TransformerTypes.ADD_TAGS, label: 'Add Tags' },
    { value: TransformerTypes.ADD_TERMS, label: 'Add Glossary Terms' },
    { value: TransformerTypes.ADD_DOMAIN, label: 'Add Domain' },
];

interface Props {
    transformer: Transformer;
    existingTransformerTypes: (string | null)[];
    index: number;
    setTransformers: React.Dispatch<React.SetStateAction<Transformer[]>>;
}

export default function TransformerInput(props: Props) {
    const { transformer, existingTransformerTypes, index, setTransformers } = props;

    function updateType(value: string) {
        setTransformers((prevTransformers) => {
            return prevTransformers.map((t, i) => {
                if (i === index) return { type: value, urns: [] };
                return t;
            });
        });
    }

    function updateUrns(urns: string[]) {
        setTransformers((prevTransformers) => {
            return prevTransformers.map((t, i) => {
                if (i === index) return { ...t, urns };
                return t;
            });
        });
    }

    function deleteTransformer() {
        setTransformers((prevTransformers) => {
            const filteredTransformers = prevTransformers.filter((_t, i) => i !== index);
            return filteredTransformers;
        });
    }

    return (
        <TransformerWrapper>
            <InputWrapper>
                <StyledSelect value={transformer.type || ''} onChange={(e) => updateType(e as string)}>
                    {typeOptions.map((option) => (
                        <Select.Option
                            key={option.value}
                            disabled={existingTransformerTypes.includes(option.value)}
                            value={option.value}
                        >
                            {option.label}
                        </Select.Option>
                    ))}
                </StyledSelect>
                {transformer.type && (
                    <EntitySearchWrapper>
                        <EntitySearchInput
                            selectedUrns={transformer.urns}
                            placeholder={getPlaceholderText(transformer.type)}
                            entityTypes={getEntityTypes(transformer.type)}
                            onChangeSelectedUrns={updateUrns}
                        />
                    </EntitySearchWrapper>
                )}
            </InputWrapper>
            <DeleteButton onClick={deleteTransformer} type="text" shape="circle" danger>
                <DeleteOutlined />
            </DeleteButton>
        </TransformerWrapper>
    );
}
