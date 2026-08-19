import { ArrowSquareOut } from '@phosphor-icons/react/dist/csr/ArrowSquareOut';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled, { DefaultTheme } from 'styled-components';

import ChangeEventComponent from '@app/entityV2/shared/tabs/Dataset/Schema/history/ChangeEvent';
import { formatTimestamp } from '@app/entityV2/shared/tabs/Dataset/Schema/history/historyUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';

import { ChangeTransaction, DataPlatform } from '@types';

// ─── Shared timeline chrome ───────────────────────────────────────────────────

const TitleText = styled.span`
    color: ${(props) => props.theme.colors.text};
    font-size: 13px;
    font-style: normal;
    font-weight: 600;
    line-height: 16px;
`;

const ChangeTransactionTimestamp = styled(TitleText)`
    background: ${(props) => props.theme.colors.bgSurface};
    border-radius: 20px;
    padding: 5px 15px;
`;

const ActorText = styled.span`
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 12px;
    font-style: italic;
    font-weight: 400;
`;

const ChangeTransactionContainer = styled.div`
    display: flex;
    flex-direction: row;
    width: 100%;
`;

const ChangeTransactionSidebar = styled.div`
    display: flex;
    flex-direction: column;
    width: 2px;
    margin-right: -2px;
    min-height: 100%;
`;

const ChangeTransactionMainContent = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
    min-height: 100%;
    padding-bottom: 36px;
`;

const ChangeTransactionTitle = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    margin-left: 15px;
`;

const TransactionDateHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
`;

const ChangeEventCircle = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 14px;
    height: 14px;
    border-radius: 50%;
    background-color: ${(props) => props.theme.colors.bgSurface};
    margin-left: -3px;
`;

const InnerEventCircle = styled.div`
    display: flex;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background-color: ${(props) => props.theme.colors.bgSurface};
`;

const ChangeEventVerticalLine = styled.div`
    width: 2px;
    height: 100%;
    margin-left: 3px;
    background-color: ${(props) => props.theme.colors.bgSurface};
`;

// ─── Version milestone styles ─────────────────────────────────────────────────

const VersionMilestoneCircle = styled.div<{ isCurrent?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 18px;
    height: 18px;
    border-radius: 50%;
    background: ${({ isCurrent, theme }) => (isCurrent ? theme.colors.borderBrand : theme.colors.bgSurfaceBrand)};
    border: 2px solid ${({ theme }) => theme.colors.borderBrand};
    margin-left: -5px;
    flex-shrink: 0;
`;

const VersionMilestoneInner = styled.div<{ isCurrent?: boolean }>`
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: ${({ isCurrent, theme }) => (isCurrent ? theme.colors.bgSurface : theme.colors.borderBrand)};
`;

const VersionMilestoneCard = styled.div<{ isCurrent?: boolean }>`
    margin-left: 14px;
    margin-top: 2px;
    padding: 10px 14px;
    background: ${({ isCurrent, theme }) =>
        isCurrent ? theme.colors.bgSurfaceBrandHover : theme.colors.bgSurfaceBrand};
    border: 1px solid ${({ theme }) => theme.colors.borderBrand};
    border-left: 3px solid ${({ theme }) => theme.colors.borderBrand};
    border-radius: 0 6px 6px 0;
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const VersionTagRow = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
`;

const VersionTagChip = styled.span<{ isCurrent?: boolean }>`
    display: inline-flex;
    align-items: center;
    background: ${({ isCurrent, theme }) => (isCurrent ? theme.colors.borderBrand : theme.colors.bgSurfaceBrandHover)};
    color: ${({ isCurrent, theme }) => (isCurrent ? theme.colors.bgSurface : theme.colors.textBrand)};
    font-size: 11px;
    font-weight: 700;
    padding: 2px 9px;
    border-radius: 100px;
    letter-spacing: 0.03em;
`;

const VersionLabel = styled.span`
    font-size: 12px;
    font-weight: 600;
    color: ${({ theme }) => theme.colors.text};
`;

const VersionComment = styled.span`
    font-size: 11.5px;
    color: ${({ theme }) => theme.colors.textSecondary};
    font-style: italic;
    line-height: 1.45;
`;

const VersionMetaRow = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
    margin-top: 2px;
`;

/** Resolves the per-stage theme palette tuple. PUBLISHED maps to success-tinted surface,
 *  DEPRECATED to warning-tinted, anything else (DRAFT, unknown) to neutral.
 */
function stageTokens(theme: DefaultTheme, stage?: string | null) {
    if (stage === 'PUBLISHED')
        return {
            bg: theme.colors.bgSurfaceSuccess,
            fg: theme.colors.textOnSurfaceSuccess,
            border: theme.colors.borderSuccess,
        };
    if (stage === 'DEPRECATED')
        return {
            bg: theme.colors.bgSurfaceWarning,
            fg: theme.colors.textOnSurfaceWarning,
            border: theme.colors.borderWarning,
        };
    return { bg: theme.colors.bgSurface, fg: theme.colors.textTertiary, border: theme.colors.border };
}

const VersionStageBadge = styled.span<{ stage?: string | null }>`
    display: inline-flex;
    align-items: center;
    font-size: 10px;
    font-weight: 700;
    padding: 1px 7px;
    border-radius: 100px;
    letter-spacing: 0.04em;
    background: ${({ stage, theme }) => stageTokens(theme, stage).bg};
    color: ${({ stage, theme }) => stageTokens(theme, stage).fg};
    border: 1px solid ${({ stage, theme }) => stageTokens(theme, stage).border};
`;

const ViewVersionLink = styled(Link)`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 11px;
    font-weight: 600;
    color: ${({ theme }) => theme.colors.textBrand};
    text-decoration: none;
    margin-left: auto;

    &:hover {
        color: ${({ theme }) => theme.colors.textHover};
        text-decoration: underline;
    }
`;

const CurrentBadge = styled.span`
    font-size: 10px;
    font-weight: 700;
    color: ${({ theme }) => theme.colors.textBrand};
    background: transparent;
    padding: 0;
`;

// ─── Inline attribution tag (used in all-versions mode) ───────────────────────

const InlineVersionTag = styled.span`
    display: inline-flex;
    align-items: center;
    background: ${({ theme }) => theme.colors.bgSurfaceBrand};
    color: ${({ theme }) => theme.colors.textBrand};
    font-size: 10px;
    font-weight: 700;
    padding: 1px 7px;
    border-radius: 100px;
    letter-spacing: 0.03em;
    border: 1px solid ${({ theme }) => theme.colors.borderBrand};
`;

// ─── Types ────────────────────────────────────────────────────────────────────

/** Extra data attached to a timeline entry that represents a version milestone. */
export interface VersionEntry {
    /** GlossaryTerm URN for this version — used for navigation. */
    urn: string;
    /** Human-readable version tag, e.g. "FY2024", "v2". */
    tag: string;
    /** Optional comment explaining what changed in this version. */
    comment?: string | null;
    /** True when this version is the one currently being viewed. */
    isCurrent: boolean;
    /** Whether this version has isLatest=true in the VersionSet. */
    isLatest: boolean;
    /** Lifecycle stage URN last segment, e.g. "DRAFT", "PUBLISHED", "DEPRECATED". */
    lifecycleStage?: string | null;
    /** Absolute path to navigate to this version's entity page. */
    entityPath?: string;
}

export interface ChangeTransactionEntry {
    transaction: ChangeTransaction;
    semanticVersion?: string;
    platform?: DataPlatform;
    nameMap?: Map<string, string>;
    /** When present, this entry renders as a version-creation milestone instead of a change list. */
    versionEntry?: VersionEntry;
    /** In all-versions mode, the version this non-milestone event belongs to. */
    ownerVersion?: VersionEntry;
    /**
     * Client-computed previous description for Documentation ADD events in all-versions mode.
     * Carries the last known description from a prior version so the diff shows what changed
     * between versions rather than an all-green "added" view.
     */
    inheritedPreviousDescription?: string;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function extractActorName(actorUrn?: string | null): string | null {
    if (!actorUrn) return null;
    const parts = actorUrn.split(':');
    return parts[parts.length - 1] || null;
}

function lifecycleLabel(stage?: string | null): string | null {
    if (!stage) return null;
    const seg = stage.split(':').pop() ?? stage;
    return seg.charAt(0) + seg.slice(1).toLowerCase();
}

// ─── Version milestone ────────────────────────────────────────────────────────

function VersionMilestoneView({
    transaction,
    versionEntry,
}: {
    transaction: ChangeTransaction;
    versionEntry: VersionEntry;
}) {
    const { t } = useTranslation('entity.profile.schema');
    const actorName = extractActorName(transaction.actor);
    const { isCurrent, tag, comment, isLatest, lifecycleStage, entityPath } = versionEntry;
    const stageSegment = lifecycleStage ? (lifecycleStage.split(':').pop() ?? lifecycleStage) : null;

    return (
        <ChangeTransactionContainer>
            <ChangeTransactionSidebar>
                <ChangeEventVerticalLine />
            </ChangeTransactionSidebar>
            <ChangeTransactionMainContent>
                <TransactionDateHeader>
                    <VersionMilestoneCircle isCurrent={isCurrent}>
                        <VersionMilestoneInner isCurrent={isCurrent} />
                    </VersionMilestoneCircle>
                    <ChangeTransactionTitle>
                        <ChangeTransactionTimestamp>
                            {formatTimestamp(transaction.timestampMillis)}
                        </ChangeTransactionTimestamp>
                        {actorName && <ActorText>{t('historyTransaction.byActor', { actorName })}</ActorText>}
                    </ChangeTransactionTitle>
                </TransactionDateHeader>

                <VersionMilestoneCard isCurrent={isCurrent}>
                    <VersionTagRow>
                        <VersionLabel>{t('versionMilestone.created')}</VersionLabel>
                        <VersionTagChip isCurrent={isCurrent}>{tag}</VersionTagChip>
                        {isCurrent && <CurrentBadge>{t('versionMilestone.viewing')}</CurrentBadge>}
                        {entityPath && !isCurrent && (
                            <ViewVersionLink to={entityPath}>
                                {t('versionMilestone.view')} <ArrowSquareOut size={11} />
                            </ViewVersionLink>
                        )}
                    </VersionTagRow>
                    {comment && <VersionComment>{comment}</VersionComment>}
                    <VersionMetaRow>
                        {stageSegment && (
                            <VersionStageBadge stage={stageSegment}>{lifecycleLabel(stageSegment)}</VersionStageBadge>
                        )}
                        {isLatest && !stageSegment && (
                            <VersionStageBadge stage="PUBLISHED">{t('versionMilestone.latest')}</VersionStageBadge>
                        )}
                    </VersionMetaRow>
                </VersionMilestoneCard>
            </ChangeTransactionMainContent>
        </ChangeTransactionContainer>
    );
}

// ─── Main component ───────────────────────────────────────────────────────────

export default function ChangeTransactionView({
    transaction,
    platform,
    semanticVersion,
    nameMap,
    versionEntry,
    ownerVersion,
    inheritedPreviousDescription,
}: ChangeTransactionEntry) {
    const { t } = useTranslation('entity.profile.schema');

    // Render as a version milestone if version metadata is attached
    if (versionEntry) {
        return <VersionMilestoneView transaction={transaction} versionEntry={versionEntry} />;
    }

    const actorName = extractActorName(transaction.actor);

    return (
        <ChangeTransactionContainer>
            <ChangeTransactionSidebar>
                <ChangeEventVerticalLine />
            </ChangeTransactionSidebar>
            <ChangeTransactionMainContent>
                <TransactionDateHeader>
                    <ChangeEventCircle>
                        <InnerEventCircle />
                    </ChangeEventCircle>
                    <ChangeTransactionTitle>
                        {platform && <PlatformIcon platform={platform} size={14} />}
                        <ChangeTransactionTimestamp>
                            {formatTimestamp(transaction.timestampMillis)}
                        </ChangeTransactionTimestamp>
                        {semanticVersion && <TitleText>{`(${semanticVersion})`}</TitleText>}
                        {ownerVersion && <InlineVersionTag>{ownerVersion.tag}</InlineVersionTag>}
                        {actorName && <ActorText>{t('historyTransaction.byActor', { actorName })}</ActorText>}
                    </ChangeTransactionTitle>
                </TransactionDateHeader>
                <div>
                    {transaction?.changes?.map((change) => (
                        <ChangeEventComponent
                            key={`${change.category}-${change.operation}-${change.description}`}
                            changeEvent={change}
                            nameMap={nameMap}
                            inheritedPreviousDescription={inheritedPreviousDescription}
                        />
                    ))}
                </div>
            </ChangeTransactionMainContent>
        </ChangeTransactionContainer>
    );
}
