import { Modal } from '@components';
import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { ComprehensiveImportProgress } from '@app/glossaryV2/import/shared/hooks/useComprehensiveImport';
import { ToastType, showToastMessage } from '@app/sharedV2/toastMessageUtils';

interface ImportProgressModalProps {
    visible: boolean;
    onClose: () => void;
    progress: ComprehensiveImportProgress;
}

const ModalContainer = styled.div`
    /* Modal styling is handled by DataHub Modal component */
`;

const ProgressContainer = styled.div`
    margin-bottom: 24px;
`;

const ProgressInfo = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
`;

const CustomProgressBar = styled.div<{ progress: number }>`
    width: 100%;
    height: 8px;
    background-color: ${(props) => props.theme.colors.border};
    border-radius: 4px;
    overflow: hidden;

    &::after {
        content: '';
        display: block;
        width: ${(props) => props.progress}%;
        height: 100%;
        background-color: ${(props) => props.theme.colors.iconInformation};
        border-radius: 4px;
        transition: width 0.3s ease;
    }
`;

const ProgressText = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const CurrentOperation = styled.div`
    background: ${(props) => props.theme.colors.bgSurface};
    padding: 12px;
    border-radius: 6px;
    margin-bottom: 16px;
`;

const ErrorList = styled.div`
    max-height: 200px;
    overflow-y: auto;
`;

const ErrorItem = styled.div`
    padding: 8px 12px;
    background: ${(props) => props.theme.colors.bgSurfaceError};
    border: 1px solid ${(props) => props.theme.colors.borderError};
    border-radius: 6px;
    margin-bottom: 8px;

    &:last-child {
        margin-bottom: 0;
    }
`;

const WarningList = styled.div`
    max-height: 200px;
    overflow-y: auto;
`;

const WarningItem = styled.div`
    padding: 8px 12px;
    background: ${(props) => props.theme.colors.bgSurfaceWarning};
    border: 1px solid ${(props) => props.theme.colors.borderWarning};
    border-radius: 6px;
    margin-bottom: 8px;

    &:last-child {
        margin-bottom: 0;
    }
`;

export const ImportProgressModal: React.FC<ImportProgressModalProps> = ({ visible, onClose, progress }) => {
    const theme = useTheme();
    const { t } = useTranslation('governance.glossary');
    const progressPercent = progress.total > 0 ? Math.round((progress.processed / progress.total) * 100) : 0;
    const hasErrors = progress.errors.length > 0;
    const hasWarnings = progress.warnings.length > 0;
    const isCompleted = progress.processed === progress.total && progress.total > 0;
    const hasFailed = progress.failed > 0;

    useEffect(() => {
        if (isCompleted) {
            showToastMessage(
                hasFailed ? ToastType.ERROR : ToastType.SUCCESS,
                hasFailed
                    ? t('import.progress.completedWithErrors')
                    : t('import.progress.completedSuccessfully'),
                3,
            );
        }
    }, [isCompleted, hasFailed, t]);

    return (
        <Modal open={visible} onCancel={onClose} width={600} title={t('import.progress.title')} footer={null}>
            <ModalContainer>
                <ProgressContainer>
                    <ProgressInfo>
                        <ProgressText>
                            {t('import.progress.entitiesProcessed', {
                                processed: progress.processed,
                                total: progress.total,
                            })}
                        </ProgressText>
                        <ProgressText>{t('import.progress.percentComplete', { percent: progressPercent })}</ProgressText>
                    </ProgressInfo>

                    <CustomProgressBar progress={progressPercent} />
                </ProgressContainer>

                {progress.currentEntity && (
                    <CurrentOperation>
                        <strong>{t('import.progress.currentOperation')}</strong>
                        <br />
                        {progress.currentPhase}
                        <br />
                        <span style={{ color: theme.colors.textSecondary }}>
                            {t('import.progress.entityLabel', { name: progress.currentEntity.name })}
                        </span>
                    </CurrentOperation>
                )}

                {hasWarnings && (
                    <div style={{ marginBottom: '16px' }}>
                        <h5
                            style={{
                                color: theme.colors.textWarning,
                                margin: '0 0 8px 0',
                                fontSize: '14px',
                                fontWeight: 600,
                            }}
                        >
                            {t('import.progress.warningsHeading', { count: progress.warnings.length })}
                        </h5>
                        <WarningList>
                            {progress.warnings.slice(0, 5).map((warning, index) => (
                                <WarningItem key={warning.entityId || index}>
                                    <strong>{warning.entityName || t('import.progress.importLabel')}</strong>
                                    <br />
                                    <span style={{ color: theme.colors.textSecondary }}>{warning.message}</span>
                                </WarningItem>
                            ))}
                            {progress.warnings.length > 5 && (
                                <span style={{ color: theme.colors.textSecondary }}>
                                    {t('import.progress.moreWarnings', { count: progress.warnings.length - 5 })}
                                </span>
                            )}
                        </WarningList>
                    </div>
                )}

                {hasErrors && (
                    <div style={{ marginBottom: '16px' }}>
                        <h5
                            style={{
                                color: theme.colors.textError,
                                margin: '0 0 8px 0',
                                fontSize: '14px',
                                fontWeight: 600,
                            }}
                        >
                            {t('import.progress.errorsHeading', { count: progress.errors.length })}
                        </h5>
                        <ErrorList>
                            {progress.errors.slice(0, 5).map((error) => (
                                <ErrorItem key={error.entityId || error.error}>
                                    <strong>{error.entityName}</strong>
                                    <br />
                                    <span style={{ color: theme.colors.textSecondary }}>{error.error}</span>
                                </ErrorItem>
                            ))}
                            {progress.errors.length > 5 && (
                                <span style={{ color: theme.colors.textSecondary }}>
                                    {t('import.progress.moreErrors', { count: progress.errors.length - 5 })}
                                </span>
                            )}
                        </ErrorList>
                    </div>
                )}
            </ModalContainer>
        </Modal>
    );
};
