import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { vi } from 'vitest';

import {
    StructuredReport,
    distributeVisibleItems,
    generateReportSubtitle,
    generateReportTitle,
    hasSomethingToShow,
} from '@app/ingestV2/executions/components/reporting/StructuredReport';
import {
    StructuredReportItemLevel,
    StructuredReportLogEntry,
} from '@app/ingestV2/executions/components/reporting/types';

// Mock the ShowMoreSection component
vi.mock('@app/shared/ShowMoreSection', () => ({
    ShowMoreSection: ({ totalCount, visibleCount, setVisibleCount }: any) => (
        <button type="button" onClick={() => setVisibleCount(visibleCount + 3)} data-testid="show-more-button">
            Show {totalCount - visibleCount} more
        </button>
    ),
}));

// Mock the StructuredReportItemList component
vi.mock('../StructuredReportItemList', () => ({
    StructuredReportItemList: ({ items, color, textColor, icon }: any) => (
        <div data-testid={`item-list-${icon}`} data-color={color} data-text-color={textColor}>
            {items.map((item: any, _index: number) => (
                <div key={`item-${item.title}-${item.message}`} data-testid="report-item">
                    {item.title}: {item.message}
                </div>
            ))}
        </div>
    ),
}));

// Mock the components module
vi.mock('@components', () => ({
    Card: ({ title, subTitle, button, children }: any) => (
        <div data-testid="mock-card">
            <div data-testid="card-title">{title}</div>
            <div data-testid="card-subtitle">{subTitle}</div>
            {button && <div data-testid="card-button">{button}</div>}
            <div data-testid="card-content">{children}</div>
        </div>
    ),
    colors: {
        red: { 0: '#FBF3EF', 1000: '#C4360B' },
        yellow: { 0: '#FFFAEB', 1000: '#C77100' },
        gray: { 1500: '#F9FAFC', 1700: '#5F6685' },
    },
    Icon: ({ icon, ...props }: any) => (
        <span data-testid={`icon-${icon}`} data-icon={icon} {...props}>
            {icon}
        </span>
    ),
}));

const createMockItem = (
    level: StructuredReportItemLevel,
    title: string,
    message: string,
): StructuredReportLogEntry => ({
    level,
    title,
    message,
    context: [],
});

describe('StructuredReport Utility Functions', () => {
    describe('generateReportTitle', () => {
        it('should return "Errors & Warnings" when all three types are present', () => {
            expect(generateReportTitle(true, true, true)).toBe('Errors & Warnings');
        });

        it('should return "Errors & Warnings" when errors and warnings are present', () => {
            expect(generateReportTitle(true, true, false)).toBe('Errors & Warnings');
        });

        it('should return "Errors & Infos" when errors and infos are present', () => {
            expect(generateReportTitle(true, false, true)).toBe('Errors & Infos');
        });

        it('should return "Warnings & Infos" when warnings and infos are present', () => {
            expect(generateReportTitle(false, true, true)).toBe('Warnings & Infos');
        });

        it('should return single type names', () => {
            expect(generateReportTitle(true, false, false)).toBe('Errors');
            expect(generateReportTitle(false, true, false)).toBe('Warnings');
            expect(generateReportTitle(false, false, true)).toBe('Infos');
        });

        it('should return empty string when nothing is present', () => {
            expect(generateReportTitle(false, false, false)).toBe('');
        });
    });

    describe('generateReportSubtitle', () => {
        it('should generate subtitle for single type', () => {
            expect(generateReportSubtitle(true, false, false)).toBe('Ingestion ran with errors.');
            expect(generateReportSubtitle(false, true, false)).toBe('Ingestion ran with warnings.');
            expect(generateReportSubtitle(false, false, true)).toBe('Ingestion ran with information.');
        });

        it('should generate subtitle for two types', () => {
            expect(generateReportSubtitle(true, true, false)).toBe('Ingestion ran with errors and warnings.');
            expect(generateReportSubtitle(true, false, true)).toBe('Ingestion ran with errors and information.');
            expect(generateReportSubtitle(false, true, true)).toBe('Ingestion ran with warnings and information.');
        });

        it('should generate subtitle for all three types', () => {
            expect(generateReportSubtitle(true, true, true)).toBe(
                'Ingestion ran with errors, warnings, and information.',
            );
        });
    });

    describe('distributeVisibleItems', () => {
        const errors = [
            createMockItem(StructuredReportItemLevel.ERROR, 'Error 1', 'First error'),
            createMockItem(StructuredReportItemLevel.ERROR, 'Error 2', 'Second error'),
        ];
        const warnings = [
            createMockItem(StructuredReportItemLevel.WARN, 'Warning 1', 'First warning'),
            createMockItem(StructuredReportItemLevel.WARN, 'Warning 2', 'Second warning'),
        ];
        const infos = [createMockItem(StructuredReportItemLevel.INFO, 'Info 1', 'First info')];

        it('should prioritize errors first', () => {
            const result = distributeVisibleItems(errors, warnings, infos, 2);
            expect(result.visibleErrors).toHaveLength(2);
            expect(result.visibleWarnings).toHaveLength(0);
            expect(result.visibleInfos).toHaveLength(0);
            expect(result.totalVisible).toBe(2);
        });

        it('should show warnings after errors are exhausted', () => {
            const result = distributeVisibleItems(errors, warnings, infos, 4);
            expect(result.visibleErrors).toHaveLength(2);
            expect(result.visibleWarnings).toHaveLength(2);
            expect(result.visibleInfos).toHaveLength(0);
            expect(result.totalVisible).toBe(4);
        });

        it('should show infos after errors and warnings are exhausted', () => {
            const result = distributeVisibleItems(errors, warnings, infos, 5);
            expect(result.visibleErrors).toHaveLength(2);
            expect(result.visibleWarnings).toHaveLength(2);
            expect(result.visibleInfos).toHaveLength(1);
            expect(result.totalVisible).toBe(5);
        });

        it('should handle visibleCount larger than total items', () => {
            const result = distributeVisibleItems(errors, warnings, infos, 10);
            expect(result.visibleErrors).toHaveLength(2);
            expect(result.visibleWarnings).toHaveLength(2);
            expect(result.visibleInfos).toHaveLength(1);
            expect(result.totalVisible).toBe(5);
        });

        it('should handle empty arrays', () => {
            const result = distributeVisibleItems([], [], [], 3);
            expect(result.visibleErrors).toHaveLength(0);
            expect(result.visibleWarnings).toHaveLength(0);
            expect(result.visibleInfos).toHaveLength(0);
            expect(result.totalVisible).toBe(0);
        });
    });

    describe('hasSomethingToShow', () => {
        it('should return true when there are items to show', () => {
            const report = {
                items: [createMockItem(StructuredReportItemLevel.ERROR, 'Error', 'Test error')],
                infoCount: 0,
                errorCount: 1,
                warnCount: 0,
            };
            expect(hasSomethingToShow(report)).toBe(true);
        });

        it('should return false when there are no items', () => {
            const report = {
                items: [],
                infoCount: 0,
                errorCount: 0,
                warnCount: 0,
            };
            expect(hasSomethingToShow(report)).toBe(false);
        });
    });
});

describe('StructuredReport Component', () => {
    const mockReport = {
        items: [
            createMockItem(StructuredReportItemLevel.ERROR, 'Error 1', 'First error'),
            createMockItem(StructuredReportItemLevel.ERROR, 'Error 2', 'Second error'),
            createMockItem(StructuredReportItemLevel.WARN, 'Warning 1', 'First warning'),
            createMockItem(StructuredReportItemLevel.WARN, 'Warning 2', 'Second warning'),
            createMockItem(StructuredReportItemLevel.INFO, 'Info 1', 'First info'),
        ],
        infoCount: 1,
        errorCount: 2,
        warnCount: 2,
    };

    it('should render nothing when report has no items', () => {
        const { container } = render(
            <StructuredReport
                report={{
                    items: [],
                    infoCount: 0,
                    errorCount: 0,
                    warnCount: 0,
                }}
            />,
        );
        expect(container.firstChild).toBeNull();
    });

    it('should render title with pill showing total count', () => {
        render(<StructuredReport report={mockReport} />);
        expect(screen.getByText('Errors & Warnings')).toBeInTheDocument();
        expect(screen.getByText('5')).toBeInTheDocument();
    });

    it('should render subtitle describing the issues', () => {
        render(<StructuredReport report={mockReport} />);
        expect(screen.getByText('Ingestion ran with errors, warnings, and information.')).toBeInTheDocument();
    });

    it('should initially show only first 3 items (errors prioritized)', () => {
        render(<StructuredReport report={mockReport} />);

        // Should show error list with 2 items
        const errorList = screen.getByTestId('item-list-WarningDiamond');
        expect(errorList).toBeInTheDocument();
        expect(errorList.children).toHaveLength(2);

        // Should show warning list with 1 item (remaining from initial 3)
        const warningList = screen.getByTestId('item-list-WarningCircle');
        expect(warningList).toBeInTheDocument();
        expect(warningList.children).toHaveLength(1);

        // Should not show info list initially
        expect(screen.queryByTestId('item-list-Info')).not.toBeInTheDocument();
    });

    it('should show 0 items by default when only warnings and infos exist', () => {
        const warningsAndInfosReport = {
            items: [
                createMockItem(StructuredReportItemLevel.WARN, 'Warning 1', 'First warning'),
                createMockItem(StructuredReportItemLevel.WARN, 'Warning 2', 'Second warning'),
                createMockItem(StructuredReportItemLevel.INFO, 'Info 1', 'First info'),
            ],
            infoCount: 1,
            errorCount: 0,
            warnCount: 2,
        };

        render(<StructuredReport report={warningsAndInfosReport} />);

        // Should not show any item lists initially
        expect(screen.queryByTestId('item-list-WarningDiamond')).not.toBeInTheDocument();
        expect(screen.queryByTestId('item-list-WarningCircle')).not.toBeInTheDocument();
        expect(screen.queryByTestId('item-list-Info')).not.toBeInTheDocument();

        // Should show "Show 3 more" button
        expect(screen.getByTestId('show-more-button')).toBeInTheDocument();
        expect(screen.getByText('Show 3 more')).toBeInTheDocument();
    });

    it('should show 0 items by default when only warnings exist', () => {
        const warningsOnlyReport = {
            items: [
                createMockItem(StructuredReportItemLevel.WARN, 'Warning 1', 'First warning'),
                createMockItem(StructuredReportItemLevel.WARN, 'Warning 2', 'Second warning'),
            ],
            infoCount: 0,
            errorCount: 0,
            warnCount: 2,
        };

        render(<StructuredReport report={warningsOnlyReport} />);

        // Should not show any item lists initially
        expect(screen.queryByTestId('item-list-WarningDiamond')).not.toBeInTheDocument();
        expect(screen.queryByTestId('item-list-WarningCircle')).not.toBeInTheDocument();
        expect(screen.queryByTestId('item-list-Info')).not.toBeInTheDocument();

        // Should show "Show 2 more" button
        expect(screen.getByTestId('show-more-button')).toBeInTheDocument();
        expect(screen.getByText('Show 2 more')).toBeInTheDocument();
    });

    it('should show 0 items by default when only infos exist', () => {
        const infosOnlyReport = {
            items: [
                createMockItem(StructuredReportItemLevel.INFO, 'Info 1', 'First info'),
                createMockItem(StructuredReportItemLevel.INFO, 'Info 2', 'Second info'),
            ],
            infoCount: 2,
            errorCount: 0,
            warnCount: 0,
        };

        render(<StructuredReport report={infosOnlyReport} />);

        // Should not show any item lists initially
        expect(screen.queryByTestId('item-list-WarningDiamond')).not.toBeInTheDocument();
        expect(screen.queryByTestId('item-list-WarningCircle')).not.toBeInTheDocument();
        expect(screen.queryByTestId('item-list-Info')).not.toBeInTheDocument();

        // Should show "Show 2 more" button
        expect(screen.getByTestId('show-more-button')).toBeInTheDocument();
        expect(screen.getByText('Show 2 more')).toBeInTheDocument();
    });

    it('should show "Show X more" button when there are more items', () => {
        render(<StructuredReport report={mockReport} />);
        expect(screen.getByTestId('show-more-button')).toBeInTheDocument();
        expect(screen.getByText('Show 2 more')).toBeInTheDocument();
    });

    it('should expand to show more items when "Show more" is clicked', async () => {
        const user = userEvent.setup();
        render(<StructuredReport report={mockReport} />);

        const showMoreButton = screen.getByTestId('show-more-button');
        await user.click(showMoreButton);

        // Should now show all items
        expect(screen.getByTestId('item-list-WarningDiamond').children).toHaveLength(2);
        expect(screen.getByTestId('item-list-WarningCircle').children).toHaveLength(2);
        expect(screen.getByTestId('item-list-Info')).toBeInTheDocument();
        expect(screen.getByTestId('item-list-Info').children).toHaveLength(1);

        // Show more button should be gone
        expect(screen.queryByTestId('show-more-button')).not.toBeInTheDocument();
    });

    it('should use correct colors for each item type', () => {
        render(<StructuredReport report={mockReport} />);

        const errorList = screen.getByTestId('item-list-WarningDiamond');
        expect(errorList).toHaveAttribute('data-color', '#FBF3EF'); // ERROR_COLOR
        expect(errorList).toHaveAttribute('data-text-color', '#C4360B'); // ERROR_TEXT_COLOR

        const warningList = screen.getByTestId('item-list-WarningCircle');
        expect(warningList).toHaveAttribute('data-color', '#FFFAEB'); // WARNING_COLOR
        expect(warningList).toHaveAttribute('data-text-color', '#C77100'); // WARNING_TEXT_COLOR
    });

    describe('Chevron Expand/Collapse Functionality', () => {
        it('should render chevron button with correct initial state', () => {
            render(<StructuredReport report={mockReport} />);

            // Should have chevron down (collapsed) initially
            const cardButton = screen.getByTestId('card-button');
            expect(cardButton).toBeInTheDocument();

            // Check for caret down icon (collapsed state)
            const caretIcon = screen.getByTestId('icon-CaretDown');
            expect(caretIcon).toBeInTheDocument();
        });

        it('should auto-switch chevron to expanded when "Show more" reaches full expansion', async () => {
            const user = userEvent.setup();
            render(<StructuredReport report={mockReport} />);

            // Initially chevron should be down (collapsed)
            expect(screen.getByTestId('icon-CaretDown')).toBeInTheDocument();

            // Click "Show more" to reach full expansion
            const showMoreButton = screen.getByTestId('show-more-button');
            await user.click(showMoreButton);

            // Chevron should auto-switch to expanded (up)
            expect(screen.getByTestId('icon-CaretUp')).toBeInTheDocument();

            // Show more button should be gone
            expect(screen.queryByTestId('show-more-button')).not.toBeInTheDocument();

            // All items should be visible
            expect(screen.getByTestId('item-list-WarningDiamond').children).toHaveLength(2);
            expect(screen.getByTestId('item-list-WarningCircle').children).toHaveLength(2);
            expect(screen.getByTestId('item-list-Info')).toBeInTheDocument();
        });

        it('should maintain pill count as total regardless of expansion state', async () => {
            const user = userEvent.setup();
            render(<StructuredReport report={mockReport} />);

            // Pill should always show total (5)
            expect(screen.getByText('5')).toBeInTheDocument();

            // Expand via chevron
            const cardButton = screen.getByTestId('card-button');
            await user.click(cardButton);

            // Pill should still show total
            expect(screen.getByText('5')).toBeInTheDocument();

            // Collapse again
            await user.click(cardButton);

            // Pill should still show total
            expect(screen.getByText('5')).toBeInTheDocument();
        });

    });
});
