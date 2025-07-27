import { Card, Icon, colors } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { StructuredReportItemList } from '@app/ingestV2/executions/components/reporting/StructuredReportItemList';
import {
    StructuredReportItemLevel,
    StructuredReportLogEntry,
    StructuredReport as StructuredReportType,
} from '@app/ingestV2/executions/components/reporting/types';
import { ShowMoreSection } from '@app/shared/ShowMoreSection';

const Container = styled(Card)`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

const TitleContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const StyledPill = styled.div`
    display: flex;
    height: 18px;
    padding: 0 6px;
    justify-content: center;
    align-items: center;
    gap: 4px;
    border-radius: 200px;
    background: ${colors.gray[1500]};
    font-size: 12px;
    font-weight: 500;
    color: ${colors.gray[1700]};
`;

const ChevronButton = styled.div`
    display: flex;
    align-items: center;
    cursor: pointer;
    gap: 4px;
`;

const ChevronIcon = styled(Icon)`
    color: ${colors.gray[1700]};
    font-size: 12px;
`;

const ERROR_COLOR = colors.red[0];
const ERROR_TEXT_COLOR = colors.red[1000];
const WARNING_COLOR = colors.yellow[0];
const WARNING_TEXT_COLOR = colors.yellow[1000];
const INFO_COLOR = colors.gray[1500];
const INFO_TEXT_COLOR = colors.gray[1700];

interface Props {
    report: StructuredReportType;
}

export function hasSomethingToShow(report: StructuredReportType): boolean {
    const warnings = report.items.filter((item) => item.level === StructuredReportItemLevel.WARN);
    const errors = report.items.filter((item) => item.level === StructuredReportItemLevel.ERROR);
    const infos = report.items.filter((item) => item.level === StructuredReportItemLevel.INFO);
    return warnings.length > 0 || errors.length > 0 || infos.length > 0;
}

export function generateReportTitle(hasErrors: boolean, hasWarnings: boolean, hasInfos: boolean): string {
    if (hasErrors && hasWarnings && hasInfos) {
        return 'Errors & Warnings';
    }
    if (hasErrors && hasWarnings) {
        return 'Errors & Warnings';
    }
    if (hasErrors && hasInfos) {
        return 'Errors & Infos';
    }
    if (hasWarnings && hasInfos) {
        return 'Warnings & Infos';
    }
    if (hasErrors) {
        return 'Errors';
    }
    if (hasWarnings) {
        return 'Warnings';
    }
    if (hasInfos) {
        return 'Infos';
    }
    return '';
}

export function generateReportSubtitle(hasErrors: boolean, hasWarnings: boolean, hasInfos: boolean): string {
    const parts: string[] = [];
    if (hasErrors) parts.push('errors');
    if (hasWarnings) parts.push('warnings');
    if (hasInfos) parts.push('information');

    let subtitle = 'Ingestion ran with ';
    if (parts.length === 1) {
        subtitle += parts[0];
    } else if (parts.length === 2) {
        subtitle += parts.join(' and ');
    } else {
        subtitle += `${parts.slice(0, -1).join(', ')}, and ${parts[parts.length - 1]}`;
    }
    subtitle += '.';
    return subtitle;
}

export function distributeVisibleItems(
    errors: StructuredReportLogEntry[],
    warnings: StructuredReportLogEntry[],
    infos: StructuredReportLogEntry[],
    visibleCount: number,
) {
    let remainingCount = visibleCount;

    // Priority 1: Errors (show as many as possible up to total errors or remaining count)
    const visibleErrors = Math.min(errors.length, remainingCount);
    remainingCount -= visibleErrors;

    // Priority 2: Warnings (show as many as possible up to total warnings or remaining count)
    const visibleWarnings = Math.min(warnings.length, remainingCount);
    remainingCount -= visibleWarnings;

    // Priority 3: Infos (show remaining count)
    const visibleInfos = Math.min(infos.length, remainingCount);

    return {
        visibleErrors: errors.slice(0, visibleErrors),
        visibleWarnings: warnings.slice(0, visibleWarnings),
        visibleInfos: infos.slice(0, visibleInfos),
        totalVisible: visibleErrors + visibleWarnings + visibleInfos,
    };
}

export function StructuredReport({ report }: Props) {
    const [visibleCount, setVisibleCount] = useState(3);
    const [isExpanded, setIsExpanded] = useState(false);

    if (!report.items.length) {
        return null;
    }

    const warnings = report.items.filter((item) => item.level === StructuredReportItemLevel.WARN);
    const errors = report.items.filter((item) => item.level === StructuredReportItemLevel.ERROR);
    const infos = report.items.filter((item) => item.level === StructuredReportItemLevel.INFO);

    const hasErrors = errors.length > 0;
    const hasWarnings = warnings.length > 0;
    const hasInfos = infos.length > 0;

    const title = generateReportTitle(hasErrors, hasWarnings, hasInfos);
    const subtitle = generateReportSubtitle(hasErrors, hasWarnings, hasInfos);
    const totalItems = errors.length + warnings.length + infos.length;

    const titleWithPill = (
        <TitleContainer>
            {title}
            <StyledPill>{totalItems}</StyledPill>
        </TitleContainer>
    );

    // Determine what items to show based on expand state or visible count
    const itemsToShow = isExpanded ? totalItems : visibleCount;

    // Distribute items based on priority and items to show
    const { visibleErrors, visibleWarnings, visibleInfos, totalVisible } = distributeVisibleItems(
        errors,
        warnings,
        infos,
        itemsToShow,
    );

    const toggleExpanded = () => {
        setIsExpanded(!isExpanded);
        if (!isExpanded) {
            // When expanding, set visible count to total so "Show more" disappears
            setVisibleCount(totalItems);
        } else {
            // When collapsing, reset to initial page size
            setVisibleCount(3);
        }
    };

    // Auto-switch chevron to expanded when "Show more" reaches full expansion
    const handleSetVisibleCount = (newCount: number) => {
        setVisibleCount(newCount);
        if (newCount >= totalItems && !isExpanded) {
            setIsExpanded(true);
        }
    };

    const chevronButton = (
        <ChevronButton onClick={toggleExpanded}>
            <ChevronIcon icon={isExpanded ? 'CaretUp' : 'CaretDown'} source="phosphor" size="md" />
        </ChevronButton>
    );

    return (
        <>
            <Container title={titleWithPill} subTitle={subtitle} button={chevronButton}>
                {visibleErrors.length ? (
                    <StructuredReportItemList
                        items={visibleErrors}
                        color={ERROR_COLOR}
                        textColor={ERROR_TEXT_COLOR}
                        icon="WarningDiamond"
                    />
                ) : null}
                {visibleWarnings.length ? (
                    <StructuredReportItemList
                        items={visibleWarnings}
                        color={WARNING_COLOR}
                        textColor={WARNING_TEXT_COLOR}
                        icon="WarningCircle"
                    />
                ) : null}
                {visibleInfos.length ? (
                    <StructuredReportItemList
                        items={visibleInfos}
                        color={INFO_COLOR}
                        textColor={INFO_TEXT_COLOR}
                        icon="Info"
                    />
                ) : null}
            </Container>
            {totalItems > totalVisible && !isExpanded ? (
                <ShowMoreSection
                    totalCount={totalItems}
                    visibleCount={totalVisible}
                    setVisibleCount={handleSetVisibleCount}
                    pageSize={3}
                />
            ) : null}
        </>
    );
}
