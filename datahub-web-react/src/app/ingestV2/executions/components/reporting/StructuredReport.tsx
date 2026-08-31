import { Card, Icon } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretUp } from '@phosphor-icons/react/dist/csr/CaretUp';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import { WarningCircle } from '@phosphor-icons/react/dist/csr/WarningCircle';
import { WarningDiamond } from '@phosphor-icons/react/dist/csr/WarningDiamond';
import i18next from 'i18next';
import React, { useState } from 'react';
import styled, { useTheme } from 'styled-components';

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
    background: ${(props) => props.theme.colors.bgSurface};
    font-size: 12px;
    font-weight: 500;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const ChevronButton = styled.div`
    display: flex;
    align-items: center;
    cursor: pointer;
    gap: 4px;
`;

const ChevronIcon = styled(Icon)`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
`;

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
    if (hasErrors && hasWarnings) {
        return i18next.t('ingestion:report.titleErrorsAndWarnings');
    }
    if (hasErrors && hasInfos) {
        return i18next.t('ingestion:report.titleErrorsAndInfos');
    }
    if (hasWarnings && hasInfos) {
        return i18next.t('ingestion:report.titleWarningsAndInfos');
    }
    if (hasErrors) {
        return i18next.t('ingestion:report.titleErrors');
    }
    if (hasWarnings) {
        return i18next.t('ingestion:report.titleWarnings');
    }
    if (hasInfos) {
        return i18next.t('ingestion:report.titleInfos');
    }
    return '';
}

export function generateReportSubtitle(hasErrors: boolean, hasWarnings: boolean, hasInfos: boolean): string {
    if (hasErrors && hasWarnings && hasInfos) {
        return i18next.t('ingestion:report.subtitleErrorsWarningsInfo');
    }
    if (hasErrors && hasWarnings) {
        return i18next.t('ingestion:report.subtitleErrorsWarnings');
    }
    if (hasErrors && hasInfos) {
        return i18next.t('ingestion:report.subtitleErrorsInfo');
    }
    if (hasWarnings && hasInfos) {
        return i18next.t('ingestion:report.subtitleWarningsInfo');
    }
    if (hasErrors) {
        return i18next.t('ingestion:report.subtitleErrors');
    }
    if (hasWarnings) {
        return i18next.t('ingestion:report.subtitleWarnings');
    }
    if (hasInfos) {
        return i18next.t('ingestion:report.subtitleInfo');
    }
    return '';
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
    const theme = useTheme();
    const errorColor = theme.colors.bgSurfaceError;
    const errorTextColor = theme.colors.textError;
    const warningColor = theme.colors.bgSurfaceWarning;
    const warningTextColor = theme.colors.textWarning;
    const infoColor = theme.colors.bgSurfaceBrand;
    const infoTextColor = theme.colors.textBrand;

    const warnings = report.items.filter((item) => item.level === StructuredReportItemLevel.WARN);
    const errors = report.items.filter((item) => item.level === StructuredReportItemLevel.ERROR);
    const infos = report.items.filter((item) => item.level === StructuredReportItemLevel.INFO);

    const hasErrors = errors.length > 0;
    const hasWarnings = warnings.length > 0;
    const hasInfos = infos.length > 0;

    // Show 0 items by default if only warnings and infos exist, otherwise show 3
    const defaultVisibleCount = !hasErrors && (hasWarnings || hasInfos) ? 0 : 3;
    const [visibleCount, setVisibleCount] = useState(defaultVisibleCount);
    const [isExpanded, setIsExpanded] = useState(false);

    if (!report.items.length) {
        return null;
    }

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
            setVisibleCount(defaultVisibleCount);
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
            <ChevronIcon icon={isExpanded ? CaretUp : CaretDown} size="md" />
        </ChevronButton>
    );

    return (
        <>
            <Container title={titleWithPill} subTitle={subtitle} button={chevronButton}>
                {visibleErrors.length ? (
                    <StructuredReportItemList
                        items={visibleErrors}
                        color={errorColor}
                        textColor={errorTextColor}
                        icon={WarningDiamond}
                    />
                ) : null}
                {visibleWarnings.length ? (
                    <StructuredReportItemList
                        items={visibleWarnings}
                        color={warningColor}
                        textColor={warningTextColor}
                        icon={WarningCircle}
                    />
                ) : null}
                {visibleInfos.length ? (
                    <StructuredReportItemList
                        items={visibleInfos}
                        color={infoColor}
                        textColor={infoTextColor}
                        icon={Info}
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
