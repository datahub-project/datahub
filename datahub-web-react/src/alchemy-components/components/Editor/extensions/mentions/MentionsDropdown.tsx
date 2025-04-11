import React, { useCallback, useMemo, useState } from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { useDebounce } from 'react-use';
import { useCommands } from '@remirror/react';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';
import { AutoCompleteResultForEntity, Entity, EntityType } from '@src/types.generated';
import AutoCompleteItem from '@src/app/searchV2/autoComplete/AutoCompleteItem';
import { useDataHubMentions } from './useDataHubMentions';

const HeaderItem = styled(Typography.Text)`
    display: block;
    min-height: 32px;
    padding: 5px 12px;
`;

const OptionItem = styled.div<{ readonly active?: boolean }>`
    min-height: 32px;
    padding: 5px 12px 5px 24px;
    background: ${(props) => (props.active ? ANTD_GRAY[3] : ANTD_GRAY[1])};
    transition: background 0.3s ease;
    cursor: pointer;
    display: flex;
    align-items: center;

    &:hover {
        background: ${ANTD_GRAY[3]};
    }
`;

type Props = {
    suggestions: AutoCompleteResultForEntity[];
};

type Option = {
    header: boolean;
    type: EntityType;
    entity?: Entity;
    index?: number;
};

/** Flattens a nested entities list to a single level array */
const flattenOptions = (suggestions: AutoCompleteResultForEntity[]): Option[] => {
    let optOffset = 0;
    return suggestions.reduce<Option[]>((acc, suggestion) => {
        acc.push({ header: true, type: suggestion.type });

        const options = suggestion.entities.map((entity) => {
            return {
                index: optOffset++,
                header: false,
                type: entity.type,
                entity,
            };
        });

        return [...acc, ...options];
    }, []);
};

export const MentionsDropdown = ({ suggestions }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [options, setOptions] = useState<Option[]>([]);
    const { createDataHubMention } = useCommands();

    useDebounce(() => setOptions(flattenOptions(suggestions)), 250, [suggestions]);
    const onSubmit = useCallback(
        (item: Option) => {
            if (item.entity) {
                createDataHubMention({
                    name: entityRegistry.getDisplayName(item.type, item.entity),
                    urn: item.entity.urn,
                });
                return true;
            }
            return false;
        },
        [createDataHubMention, entityRegistry],
    );

    /** Store a separate list of options without header items for arrow keys navigation  */
    const items = useMemo(() => options.filter(({ header }) => !header), [options]);
    const { selectedIndex, filter } = useDataHubMentions<Option>({ items, onEnter: onSubmit });

    return (
        <div role="menu">
            {options.map((option) => {
                const { header, type, entity, index = 0 } = option;
                if (header) {
                    const label = entityRegistry.getCollectionName(type);
                    return (
                        <HeaderItem key={`Header_${label}`} type="secondary">
                            {label}
                        </HeaderItem>
                    );
                }

                if (!entity) return null;
                const highlight = selectedIndex === index;
                const onMouseDown = (e) => {
                    e.preventDefault();
                    onSubmit(option);
                };

                return (
                    <OptionItem active={highlight} key={entity.urn} onMouseDown={onMouseDown} role="option">
                        <AutoCompleteItem query={filter ?? ''} entity={entity} />
                    </OptionItem>
                );
            })}
        </div>
    );
};
