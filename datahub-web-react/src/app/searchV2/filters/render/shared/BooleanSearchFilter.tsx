import { Pill, Popover } from '@components';
import React, { CSSProperties, useState } from 'react';

import FilterOption from '@app/searchV2/filters/FilterOption';
import { SearchFilterBase } from '@app/searchV2/filters/SearchFilterBase';
import BooleanSearchFilterMenu from '@app/searchV2/filters/render/shared/BooleanMoreFilterMenu';

// The menu draws its own card, so the popover's default chrome is removed.
const OVERLAY_INNER_STYLE: CSSProperties = {
    padding: 0,
    background: 'transparent',
    boxShadow: 'none',
    borderRadius: 0,
    overflow: 'visible',
};

interface Props {
    title: string;
    option: string;
    count: number;
    initialSelected: boolean;
    onUpdate: (newValue: boolean) => void;
}

export default function BooleanSearchFilter({ title, option, count, initialSelected, onUpdate }: Props) {
    const [isMenuOpen, setIsMenuOpen] = useState(false);

    // A single option, so a toggle applies straight away rather than waiting for the menu to close.
    const toggle = () => {
        onUpdate(!initialSelected);
        setIsMenuOpen(false);
    };

    return (
        <Popover
            open={isMenuOpen}
            onOpenChange={setIsMenuOpen}
            trigger="click"
            placement="bottomLeft"
            overlayInnerStyle={OVERLAY_INNER_STYLE}
            content={
                <BooleanSearchFilterMenu
                    filterOption={
                        <FilterOption
                            filterOption={{ field: title, value: option, count }}
                            selectedFilterOptions={initialSelected ? [{ field: title, value: option }] : []}
                            setSelectedFilterOptions={toggle}
                        />
                    }
                />
            }
        >
            <SearchFilterBase
                isOpen={isMenuOpen}
                isActive={initialSelected}
                data-testid={`filter-dropdown-${title.replace(/\s/g, '-')}`}
                onClear={() => onUpdate(false)}
                showClear={initialSelected}
            >
                {title} {initialSelected ? <Pill label="1" size="sm" variant="filled" /> : null}
            </SearchFilterBase>
        </Popover>
    );
}
