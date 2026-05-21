import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useRef, useState } from 'react';
import styled from 'styled-components';

import FilterOption from '@app/searchV2/filters/FilterOption';
import BooleanSearchFilterMenu from '@app/searchV2/filters/render/shared/BooleanMoreFilterMenu';
import { MoreFilterOptionLabel } from '@app/searchV2/filters/styledComponents';
import { useElementDimensions } from '@app/searchV2/filters/utils';

const SubMenuWrapper = styled.div`
    position: relative;
`;

interface Props {
    title: string;
    option: string;
    count: number;
    initialSelected: boolean;
    onUpdate: (newValue: boolean) => void;
}

export default function BooleanMoreFilter({ title, option, count, initialSelected, onUpdate }: Props) {
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const [isSelected, setIsSelected] = useState<boolean>(initialSelected);
    const labelRef = useRef<HTMLDivElement>(null);
    const { width, height, isElementOutsideWindow } = useElementDimensions(labelRef);

    function updateSelected() {
        onUpdate(isSelected);
        setIsMenuOpen(false);
    }

    return (
        <SubMenuWrapper>
            <MoreFilterOptionLabel
                onClick={() => setIsMenuOpen(!isMenuOpen)}
                isOpen={isMenuOpen}
                $isActive={isSelected}
                data-testid={`more-filter-${title.replace(/\s/g, '-')}`}
                ref={labelRef}
            >
                {title} {isSelected ? `(1) ` : ''}
                <CaretRight size={12} />
            </MoreFilterOptionLabel>
            {isMenuOpen && (
                <BooleanSearchFilterMenu
                    filterOption={
                        <FilterOption
                            filterOption={{ field: title, value: option, count }}
                            selectedFilterOptions={isSelected ? [{ field: title, value: option }] : []}
                            setSelectedFilterOptions={() => setIsSelected(!isSelected)}
                        />
                    }
                    onUpdate={updateSelected}
                    style={{
                        position: 'absolute',
                        top: -height,
                        [isElementOutsideWindow ? 'right' : 'left']: width,
                    }}
                />
            )}
        </SubMenuWrapper>
    );
}
