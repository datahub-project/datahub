import { Modal, Text } from '@components';
import { diffLines, diffWords } from 'diff';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';
import { Entity, EntityData } from '@app/glossaryV2/import/glossary.types';
import {
    compareCustomProperties,
    parseCustomProperties,
} from '@app/glossaryV2/import/shared/utils/customPropertiesUtils';

type DiffVariant = 'removed' | 'added' | 'context';

interface Segment {
    text: string;
    highlighted: boolean;
}

interface DiffRow {
    num: number;
    marker: string;
    variant: DiffVariant;
    segments: Segment[];
}

interface ComparisonField {
    key: string;
    label: string;
    existingValue: string;
    importedValue: string;
    hasChanges: boolean;
}

interface DiffModalProps {
    visible: boolean;
    onClose: () => void;
    entity: Entity | null;
    existingEntity?: Entity | null;
}

const fieldLabels: Record<string, string> = {
    name: 'Name',
    parent_nodes: 'Parent Nodes',
    entity_type: 'Entity Type',
    description: 'Description',
    term_source: 'Term Source',
    source_ref: 'Source Ref',
    source_url: 'Source URL',
    ownership_users: 'Ownership (Users)',
    ownership_groups: 'Ownership (Groups)',
    related_contains: 'Related Contains',
    related_inherits: 'Related Inherits',
    domain_name: 'Domain Name',
    custom_properties: 'Custom Properties',
};

const FIELD_ORDER = [
    'name',
    'parent_nodes',
    'entity_type',
    'description',
    'term_source',
    'source_ref',
    'source_url',
    'ownership_users',
    'ownership_groups',
    'related_contains',
    'related_inherits',
    'domain_name',
    'custom_properties',
];

const Section = styled.div`
    margin-bottom: 16px;
`;

const SectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 8px;
`;

const Toggle = styled.div`
    display: flex;
    gap: 14px;
    font-size: 13px;
`;

const ToggleOption = styled.span<{ $active: boolean }>`
    cursor: pointer;
    color: ${({ theme, $active }) => ($active ? theme.colors.textInformation : theme.colors.textSecondary)};
    font-weight: ${({ $active }) => ($active ? 500 : 400)};
`;

const Split = styled.div`
    display: grid;
    grid-template-columns: 1fr 1fr;
    border: 1px solid ${({ theme }) => theme.colors.border};
    border-radius: 8px;
    overflow: hidden;
`;

const Column = styled.div<{ $right?: boolean }>`
    border-left: ${({ theme, $right }) => ($right ? `1px solid ${theme.colors.border}` : 'none')};
    min-width: 0;
`;

const Line = styled.div<{ $variant: DiffVariant }>`
    display: grid;
    grid-template-columns: 32px 16px 1fr;
    font-size: 13px;
    line-height: 1.6;
    background: ${({ theme, $variant }) => {
        if ($variant === 'removed') return theme.colors.bgSurfaceError;
        if ($variant === 'added') return theme.colors.bgSurfaceSuccess;
        return 'transparent';
    }};
`;

const Gutter = styled.span`
    text-align: right;
    padding: 2px 8px 2px 0;
    color: ${({ theme }) => theme.colors.textTertiary};
    user-select: none;
`;

const Marker = styled.span<{ $variant: DiffVariant }>`
    text-align: center;
    user-select: none;
    color: ${({ theme, $variant }) => {
        if ($variant === 'removed') return theme.colors.textError;
        if ($variant === 'added') return theme.colors.textSuccess;
        return theme.colors.textTertiary;
    }};
`;

const LineText = styled.span<{ $variant: DiffVariant }>`
    padding: 2px 8px 2px 2px;
    white-space: pre-wrap;
    word-break: break-word;
    color: ${({ theme, $variant }) => {
        if ($variant === 'removed') return theme.colors.textOnSurfaceError;
        if ($variant === 'added') return theme.colors.textOnSurfaceSuccess;
        return theme.colors.textSecondary;
    }};
`;

const Chip = styled.span<{ $variant: DiffVariant }>`
    border-radius: 3px;
    padding: 0 2px;
    font-weight: 500;
    background: ${({ theme, $variant }) =>
        $variant === 'removed' ? theme.colors.bgSurfaceErrorHover : theme.colors.bgSurfaceSuccessHover};
`;

const CompactSide = styled.div<{ $variant: DiffVariant; $right?: boolean }>`
    display: grid;
    grid-template-columns: 16px 1fr;
    padding: 6px 10px 6px 0;
    border-left: ${({ theme, $right }) => ($right ? `1px solid ${theme.colors.border}` : 'none')};
    min-width: 0;
    background: ${({ theme, $variant }) => {
        if ($variant === 'removed') return theme.colors.bgSurfaceError;
        if ($variant === 'added') return theme.colors.bgSurfaceSuccess;
        return 'transparent';
    }};
`;

const CompactValue = styled.span<{ $variant: DiffVariant }>`
    font-size: 13px;
    white-space: pre-wrap;
    word-break: break-word;
    color: ${({ theme, $variant }) => {
        if ($variant === 'removed') return theme.colors.textOnSurfaceError;
        if ($variant === 'added') return theme.colors.textOnSurfaceSuccess;
        return theme.colors.textTertiary;
    }};
    ${({ $variant }) => $variant === 'context' && 'font-style: italic;'}
`;

const MarkdownPane = styled.div<{ $variant: DiffVariant }>`
    padding: 8px 12px;
    border-left: ${({ theme, $variant }) => ($variant === 'added' ? `1px solid ${theme.colors.border}` : 'none')};
    background: ${({ theme, $variant }) =>
        $variant === 'added' ? theme.colors.bgSurfaceSuccess : theme.colors.bgSurfaceError};
    min-width: 0;
`;

const UnchangedToggle = styled.button`
    display: flex;
    align-items: center;
    gap: 6px;
    background: none;
    border: none;
    padding: 4px 0;
    cursor: pointer;
`;

const UnchangedRow = styled.div`
    display: flex;
    gap: 8px;
    padding: 3px 0;
`;

function formatCustomPropertiesForDisplay(value: string): string {
    if (!value) return '';
    try {
        const parsed = parseCustomProperties(value);
        if (Object.keys(parsed).length === 0) return '';
        return Object.entries(parsed)
            .map(([key, val]) => `${key}: ${val}`)
            .join('\n');
    } catch {
        return value;
    }
}

function ensureTrailingNewline(value: string): string {
    return value.endsWith('\n') ? value : `${value}\n`;
}

function splitToLines(value: string): string[] {
    const v = value.endsWith('\n') ? value.slice(0, -1) : value;
    return v.split('\n');
}

// Word-level segments for one side of a changed line/value. side='left' keeps removed+common
// tokens (highlighting removals); side='right' keeps added+common (highlighting additions).
function wordSegments(removedText: string, addedText: string, side: 'left' | 'right'): Segment[] {
    const parts = diffWords(removedText, addedText);
    return parts
        .filter((p) => (side === 'left' ? !p.added : !p.removed))
        .map((p) => ({ text: p.value, highlighted: side === 'left' ? !!p.removed : !!p.added }));
}

function partVariant(part: { added?: boolean; removed?: boolean }): DiffVariant {
    if (part.added) return 'added';
    if (part.removed) return 'removed';
    return 'context';
}

function buildLineDiff(existingText: string, importedText: string): { left: DiffRow[]; right: DiffRow[] } {
    const parts = diffLines(ensureTrailingNewline(existingText || ''), ensureTrailingNewline(importedText || ''));
    const blocks = parts.map((p) => ({ type: partVariant(p), lines: splitToLines(p.value) }));

    const left: DiffRow[] = [];
    const right: DiffRow[] = [];
    const pushLeft = (marker: string, variant: DiffVariant, segments: Segment[]) =>
        left.push({ num: left.length + 1, marker, variant, segments });
    const pushRight = (marker: string, variant: DiffVariant, segments: Segment[]) =>
        right.push({ num: right.length + 1, marker, variant, segments });

    let i = 0;
    while (i < blocks.length) {
        const block = blocks[i];
        const next = blocks[i + 1];
        if (block.type === 'context') {
            block.lines.forEach((line) => {
                pushLeft(' ', 'context', [{ text: line, highlighted: false }]);
                pushRight(' ', 'context', [{ text: line, highlighted: false }]);
            });
            i += 1;
        } else if (block.type === 'removed' && next && next.type === 'added') {
            // Modified region: pair removed and added lines positionally and diff words within each pair.
            const maxLen = Math.max(block.lines.length, next.lines.length);
            for (let k = 0; k < maxLen; k += 1) {
                const rem = block.lines[k];
                const add = next.lines[k];
                if (rem !== undefined && add !== undefined) {
                    pushLeft('-', 'removed', wordSegments(rem, add, 'left'));
                    pushRight('+', 'added', wordSegments(rem, add, 'right'));
                } else if (rem !== undefined) {
                    pushLeft('-', 'removed', [{ text: rem, highlighted: true }]);
                } else if (add !== undefined) {
                    pushRight('+', 'added', [{ text: add, highlighted: true }]);
                }
            }
            i += 2;
        } else if (block.type === 'removed') {
            block.lines.forEach((line) => pushLeft('-', 'removed', [{ text: line, highlighted: true }]));
            i += 1;
        } else {
            block.lines.forEach((line) => pushRight('+', 'added', [{ text: line, highlighted: true }]));
            i += 1;
        }
    }
    return { left, right };
}

const RenderSegments: React.FC<{ segments: Segment[]; variant: DiffVariant }> = ({ segments, variant }) => (
    <>
        {segments.map((seg, idx) =>
            seg.highlighted && seg.text !== '' ? (
                // eslint-disable-next-line react/no-array-index-key
                <Chip key={idx} $variant={variant}>
                    {seg.text}
                </Chip>
            ) : (
                // eslint-disable-next-line react/no-array-index-key
                <React.Fragment key={idx}>{seg.text}</React.Fragment>
            ),
        )}
    </>
);

const DiffColumn: React.FC<{ rows: DiffRow[]; right?: boolean }> = ({ rows, right }) => (
    <Column $right={right}>
        {rows.map((row, idx) => (
            // eslint-disable-next-line react/no-array-index-key
            <Line key={`${row.variant}-${row.num}-${idx}`} $variant={row.variant}>
                <Gutter>{row.num}</Gutter>
                <Marker $variant={row.variant}>{row.marker}</Marker>
                <LineText $variant={row.variant}>
                    <RenderSegments segments={row.segments} variant={row.variant} />
                </LineText>
            </Line>
        ))}
    </Column>
);

export const DiffModal: React.FC<DiffModalProps> = ({ visible, onClose, entity, existingEntity }) => {
    const { t } = useTranslation('governance.glossary');
    const [descMode, setDescMode] = useState<'markdown' | 'formatted'>('markdown');
    const [showUnchanged, setShowUnchanged] = useState(false);

    const fields = useMemo<ComparisonField[]>(() => {
        if (!entity || !entity.data) return [];
        const importedData = entity.data;
        const existingData = existingEntity?.data;

        const normalize = (value: string | undefined | null) => (value === null || value === undefined ? '' : value);
        const format = (key: string, value: string | undefined) =>
            key === 'custom_properties' ? formatCustomPropertiesForDisplay(value || '') : value || '';

        return FIELD_ORDER.map((key) => {
            const importedValue = importedData[key as keyof EntityData] as string | undefined;
            const existingValue = existingData?.[key as keyof EntityData] as string | undefined;
            const hasChanges =
                key === 'custom_properties'
                    ? !compareCustomProperties(normalize(importedValue), normalize(existingValue))
                    : normalize(importedValue) !== normalize(existingValue);
            return {
                key,
                label: fieldLabels[key] || key,
                existingValue: format(key, existingValue),
                importedValue: format(key, importedValue),
                hasChanges,
            };
        });
    }, [entity, existingEntity]);

    if (!entity) return null;

    const status = entity.status || 'new';
    const changed = fields.filter((f) => f.hasChanges);
    const unchanged = fields.filter((f) => !f.hasChanges);
    const description = changed.find((f) => f.key === 'description');
    const otherChanged = changed.filter((f) => f.key !== 'description');
    const descDiff = description ? buildLineDiff(description.existingValue, description.importedValue) : null;

    const renderCompactSide = (segments: Segment[], variant: DiffVariant, empty: boolean, right?: boolean) => {
        const effective: DiffVariant = empty ? 'context' : variant;
        let markerText = '';
        if (!empty) markerText = variant === 'removed' ? '-' : '+';
        return (
            <CompactSide $variant={effective} $right={right}>
                <Marker $variant={effective}>{markerText}</Marker>
                <CompactValue $variant={effective}>
                    {empty ? t('import.diff.noValue') : <RenderSegments segments={segments} variant={variant} />}
                </CompactValue>
            </CompactSide>
        );
    };

    return (
        <Modal
            title={`Entity Comparison: ${entity.name}`}
            subtitle={`Status: ${status.charAt(0).toUpperCase() + status.slice(1)}`}
            onCancel={onClose}
            open={visible}
            width="63%"
            dataTestId="diff-modal"
        >
            {description && descDiff && (
                <Section>
                    <SectionHeader>
                        <Text weight="medium">{description.label}</Text>
                        <Toggle>
                            <ToggleOption $active={descMode === 'markdown'} onClick={() => setDescMode('markdown')}>
                                {t('import.diff.markdown')}
                            </ToggleOption>
                            <ToggleOption $active={descMode === 'formatted'} onClick={() => setDescMode('formatted')}>
                                {t('import.diff.formattedText')}
                            </ToggleOption>
                        </Toggle>
                    </SectionHeader>
                    {descMode === 'markdown' ? (
                        <Split>
                            <DiffColumn rows={descDiff.left} />
                            <DiffColumn rows={descDiff.right} right />
                        </Split>
                    ) : (
                        <Split>
                            <MarkdownPane $variant="removed">
                                <CompactMarkdownViewer content={description.existingValue || ''} />
                            </MarkdownPane>
                            <MarkdownPane $variant="added">
                                <CompactMarkdownViewer content={description.importedValue || ''} />
                            </MarkdownPane>
                        </Split>
                    )}
                </Section>
            )}

            {otherChanged.map((field) => {
                const left = field.existingValue;
                const right = field.importedValue;
                const segs = wordSegments(left, right, 'left');
                const segsRight = wordSegments(left, right, 'right');
                return (
                    <Section key={field.key}>
                        <SectionHeader>
                            <Text weight="medium">{field.label}</Text>
                        </SectionHeader>
                        <Split>
                            {renderCompactSide(segs, 'removed', left === '')}
                            {renderCompactSide(segsRight, 'added', right === '', true)}
                        </Split>
                    </Section>
                );
            })}

            {unchanged.length > 0 && (
                <Section>
                    <UnchangedToggle onClick={() => setShowUnchanged((v) => !v)}>
                        <Text color="gray" size="sm">
                            {t('import.diff.unchanged', { count: unchanged.length })}
                        </Text>
                    </UnchangedToggle>
                    {showUnchanged &&
                        unchanged.map((field) => (
                            <UnchangedRow key={field.key}>
                                <Text color="gray" size="sm" weight="medium">
                                    {field.label}
                                </Text>
                                <Text color="gray" size="sm">
                                    {field.existingValue || t('import.diff.noValue')}
                                </Text>
                            </UnchangedRow>
                        ))}
                </Section>
            )}
        </Modal>
    );
};
