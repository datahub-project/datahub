import { useCommands } from '@remirror/react';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import { useDataHubMentions } from '@components/components/Editor/extensions/mentions/useDataHubMentions';
import { colors } from '@components/theme';

import AutoCompleteItem from '@src/app/searchV2/autoComplete/AutoCompleteItem';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { AutoCompleteResultForEntity, Entity, EntityType } from '@src/types.generated';

const HeaderItem = styled.div`
    display: block;
    min-height: 32px;
    padding: 8px 12px 4px;
    font-size: 12px;
    font-weight: 600;
    color: ${colors.gray[500]};
    text-transform: uppercase;
    letter-spacing: 0.5px;
`;

const OptionItem = styled.div<{ readonly active?: boolean }>`
    min-height: 36px;
    padding: 6px 12px 6px 16px;
    background: ${(props) => (props.active ? colors.gray[1500] : 'white')};
    transition: background 0.15s ease;
    cursor: pointer;
    display: flex;
    align-items: center;

    &:hover {
        background: ${colors.gray[1500]};
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
        (item: Option | undefined) => {
            // Guard: item may be undefined if Enter is pressed during debounce window
            if (!item?.entity) return false;
            createDataHubMention({
                name: entityRegistry.getDisplayName(item.type, item.entity),
                urn: item.entity.urn,
            });
            return true;
        },
        [createDataHubMention, entityRegistry],
    );

    /** Store a separate list of options without header items for arrow keys navigation  */
    const items = useMemo(() => options.filter(({ header }) => !header), [options]);
    const { selectedIndex, filter } = useDataHubMentions<Option>({ items, onEnter: onSubmit });

    // Ref for the currently selected item to scroll into view
    const selectedRef = useRef<HTMLDivElement>(null);

    // Scroll selected item into view when navigating with keyboard
    useEffect(() => {
        selectedRef.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }, [selectedIndex]);

    return (
        <div role="menu">
            {options.map((option) => {
                const { header, type, entity, index = 0 } = option;
                if (header) {
                    const label = entityRegistry.getCollectionName(type);
                    return <HeaderItem key={`Header_${label}`}>{label}</HeaderItem>;
                }

                if (!entity) return null;
                const highlight = selectedIndex === index;
                const onMouseDown = (e) => {
                    e.preventDefault();
                    onSubmit(option);
                };

                return (
                    <OptionItem
                        ref={highlight ? selectedRef : undefined}
                        active={highlight}
                        key={entity.urn}
                        onMouseDown={onMouseDown}
                        role="option"
                    >
                        <AutoCompleteItem query={filter ?? ''} entity={entity} />
                    </OptionItem>
                );
            })}
        </div>
    );
};
