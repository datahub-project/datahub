import { Button, Dropdown, Pill, Text, Tooltip } from '@components';
import { SlidersHorizontal } from '@phosphor-icons/react/dist/csr/SlidersHorizontal';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { radius, spacing, zIndices } from '@components/theme';

export type SidebarDisplayOption = {
    value: string;
    label: string;
};

/** One titled block of mutually exclusive options (e.g. "Grouping", "Sorting"). */
export type SidebarDisplaySection = {
    key: string;
    label: string;
    options: SidebarDisplayOption[];
    value: string;
    onChange: (value: string) => void;
};

type Props = {
    sections: SidebarDisplaySection[];
    ariaLabel?: string;
    tooltip?: string;
    dataTestId?: string;
};

const Panel = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    /* Bounded so long option labels wrap onto a second line instead of stretching the panel. */
    width: max-content;
    max-width: 260px;
    margin-top: 4px;
    padding: ${spacing.sm};
    border-radius: ${radius.lg};
    background: ${({ theme }) => theme.colors.bg};
    box-shadow: ${({ theme }) => theme.colors.shadowMd};
    z-index: ${zIndices.dropdown};
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    gap: 6px;
`;

const SectionLabel = styled(Text)`
    color: ${({ theme }) => theme.colors.text};
`;

const PillRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
`;

/**
 * Owns the trigger's hover state so the tooltip cannot strand itself: antd's
 * `mouseEnterDelay` timer can resolve *after* a click has already opened the
 * panel, re-showing the tooltip with no pending mouseleave to dismiss it.
 */
const TriggerWrapper = styled.span`
    display: inline-flex;
`;

/** Transparent hit target so pills are focusable and Enter/Space-activatable. */
const PillButton = styled.button`
    all: unset;
    display: inline-flex;
    cursor: pointer;
    border-radius: ${radius.sm};

    &:focus-visible {
        outline: 2px solid ${({ theme }) => theme.colors.borderBrand};
        outline-offset: 1px;
    }
`;

/**
 * View-shape control for HierarchicalBrowseSidebar — a single icon opening a panel
 * of "how is this list shaped" pivots (grouping, sorting, …). Each section is a
 * wrapping row of single-select square pills: gray unselected, violet selected.
 *
 * Prefer this over stacking each pivot into the filter row: filters *narrow* the
 * data, these *reshape* it, and a labelled grouping select next to a same-named
 * facet filter reads as a duplicate. Use `SidebarSortSelect` instead when a page
 * only ever exposes sort.
 *
 * Stays open on select (unlike the menu-based `SidebarSortSelect`) so grouping and
 * sorting can be changed in one visit.
 */
export default function SidebarDisplaySelect({
    sections,
    ariaLabel,
    tooltip,
    dataTestId = 'sidebar-display-select',
}: Props) {
    const { t } = useTranslation('misc');
    const [open, setOpen] = useState(false);
    const [hovered, setHovered] = useState(false);
    const resolvedAriaLabel = ariaLabel ?? t('sidebarDisplay.ariaLabel');
    const resolvedTooltip = tooltip ?? t('sidebarDisplay.tooltip');

    const handleOpenChange = useCallback((next: boolean) => {
        setOpen(next);
        if (next) {
            setHovered(false);
        }
    }, []);

    const panel = useMemo(
        () => (
            <Panel data-testid={`${dataTestId}-panel`}>
                {sections.map((section) => (
                    <Section key={section.key}>
                        <SectionLabel weight="bold" size="sm">
                            {section.label}
                        </SectionLabel>
                        <PillRow role="group" aria-label={section.label}>
                            {section.options.map((option) => {
                                const isSelected = option.value === section.value;
                                return (
                                    <PillButton
                                        key={option.value}
                                        type="button"
                                        aria-pressed={isSelected}
                                        onClick={() => section.onChange(option.value)}
                                    >
                                        <Pill
                                            label={option.label}
                                            variant="squareFilled"
                                            size="sm"
                                            color={isSelected ? 'violet' : 'gray'}
                                            clickable={false}
                                            dataTestId={`${dataTestId}-${section.key}-option-${option.value}`}
                                        />
                                    </PillButton>
                                );
                            })}
                        </PillRow>
                    </Section>
                ))}
            </Panel>
        ),
        [sections, dataTestId],
    );

    return (
        <Dropdown
            open={open}
            onOpenChange={handleOpenChange}
            trigger={['click']}
            placement="bottomRight"
            dropdownRender={() => panel}
        >
            <TriggerWrapper onMouseEnter={() => setHovered(true)} onMouseLeave={() => setHovered(false)}>
                <Tooltip
                    title={resolvedTooltip}
                    open={hovered && !open}
                    destroyTooltipOnHide
                    showArrow={false}
                    placement="bottom"
                >
                    <Button
                        variant="text"
                        color="gray"
                        size="lg"
                        isActive={open}
                        icon={{ icon: SlidersHorizontal, color: 'icon' }}
                        aria-label={resolvedAriaLabel}
                        data-testid={dataTestId}
                    />
                </Tooltip>
            </TriggerWrapper>
        </Dropdown>
    );
}
