import { Modal } from '@components';
import React, { useCallback, useMemo, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import ImportDocumentsConfigureStep from '@app/context/import/components/ImportDocumentsConfigureStep';
import ImportDocumentsImportingStep from '@app/context/import/components/ImportDocumentsImportingStep';
import ImportDocumentsResultStep from '@app/context/import/components/ImportDocumentsResultStep';
import ImportDocumentsSourceGrid from '@app/context/import/components/ImportDocumentsSourceGrid';
import { Content } from '@app/context/import/components/importDocumentsModal.styles';
import { useImportFromFiles } from '@app/context/import/hooks/useImportFromFiles';
import { useLaunchDocumentIngestionSource } from '@app/context/import/hooks/useLaunchDocumentIngestionSource';
import { ImportSourceType, ImportStep, ImportUseCase } from '@app/context/import/import.types';
import { useImportDocumentsModalButtons } from '@app/context/import/useImportDocumentsModalButtons';
import { useUserContext } from '@app/context/useUserContext';
import { useAppConfig } from '@app/useAppConfig';

type ImportDocumentsModalProps = {
    visible: boolean;
    onClose: () => void;
    useCase?: ImportUseCase;
    onSuccess?: () => void;
};

const SCHEDULED_SOURCES = new Set<ImportSourceType>([
    ImportSourceType.GITHUB,
    ImportSourceType.NOTION,
    ImportSourceType.CONFLUENCE,
]);

export default function ImportDocumentsModal({
    visible,
    onClose,
    useCase = ImportUseCase.CONTEXT_DOCUMENT,
    onSuccess,
}: ImportDocumentsModalProps) {
    const { platformPrivileges } = useUserContext();
    const appConfig = useAppConfig();
    const launchDocumentIngestionSource = useLaunchDocumentIngestionSource();

    const canImportFromScheduledSources = Boolean(
        platformPrivileges?.manageIngestion && appConfig.config.featureFlags.showIngestionPageRedesign,
    );

    const [step, setStep] = useState<ImportStep>('source');
    const [sourceType, setSourceType] = useState<ImportSourceType | null>(null);
    const [uploadFiles, setUploadFiles] = useState<File[]>([]);
    const { importFiles, error: fileError, result: fileResult } = useImportFromFiles();

    const handleClose = useCallback(() => {
        setStep('source');
        setSourceType(null);
        setUploadFiles([]);
        onClose();
    }, [onClose]);

    const handleSelectSource = useCallback(
        (source: ImportSourceType) => {
            if (SCHEDULED_SOURCES.has(source)) {
                analytics.event({
                    type: EventType.ImportDocumentsEvent,
                    source,
                    createdCount: 0,
                    updatedCount: 0,
                    failedCount: 0,
                });
                launchDocumentIngestionSource({ source });
                handleClose();
                return;
            }

            setSourceType(source);
            setStep('configure');
        },
        [launchDocumentIngestionSource, handleClose],
    );

    const handleImport = useCallback(async () => {
        setStep('importing');
        try {
            const importResult = await importFiles(uploadFiles, true, useCase);
            if (importResult && sourceType) {
                analytics.event({
                    type: EventType.ImportDocumentsEvent,
                    source: sourceType,
                    createdCount: importResult.createdCount ?? 0,
                    updatedCount: importResult.updatedCount ?? 0,
                    failedCount: importResult.failedCount ?? 0,
                });
            }
            setStep('result');
            onSuccess?.();
        } catch {
            setStep('result');
        }
    }, [sourceType, importFiles, uploadFiles, useCase, onSuccess]);

    const canImport = useMemo(
        () => sourceType === ImportSourceType.FILE_UPLOAD && uploadFiles.length > 0,
        [sourceType, uploadFiles],
    );

    const buttons = useImportDocumentsModalButtons({
        step,
        sourceType,
        canImport,
        onClose: handleClose,
        onBack: () => setStep('source'),
        onImportFiles: handleImport,
    });

    if (!visible) return null;

    return (
        <Modal
            title="Import Documents"
            onCancel={handleClose}
            buttons={buttons}
            width={560}
            dataTestId="import-documents-modal"
        >
            <Content>
                {step === 'source' && (
                    <ImportDocumentsSourceGrid
                        canImportFromScheduledSources={canImportFromScheduledSources}
                        onSelectSource={handleSelectSource}
                    />
                )}
                {step === 'configure' && sourceType === ImportSourceType.FILE_UPLOAD && (
                    <ImportDocumentsConfigureStep
                        sourceType={sourceType}
                        uploadFiles={uploadFiles}
                        onFilesChange={setUploadFiles}
                    />
                )}
                {step === 'importing' && <ImportDocumentsImportingStep />}
                {step === 'result' && (
                    <ImportDocumentsResultStep
                        importError={fileError}
                        totalImported={fileResult?.createdCount ?? 0}
                        totalUpdated={fileResult?.updatedCount ?? 0}
                        totalFailed={fileResult?.failedCount ?? 0}
                    />
                )}
            </Content>
        </Modal>
    );
}
