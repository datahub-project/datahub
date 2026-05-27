import { Modal } from '@components';
import React, { useCallback, useContext, useEffect, useMemo, useState } from 'react';

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
import { DocumentTreeContext } from '@app/document/DocumentTreeContext';
import { useAppConfig } from '@app/useAppConfig';

type ImportDocumentsModalProps = {
    visible: boolean;
    onClose: () => void;
    useCase?: ImportUseCase;
    onSuccess?: (parentUrn: string | null) => void;
    defaultParentUrn?: string | null;
};

export default function ImportDocumentsModal({
    visible,
    onClose,
    useCase = ImportUseCase.CONTEXT_DOCUMENT,
    onSuccess,
    defaultParentUrn,
}: ImportDocumentsModalProps) {
    const documentTreeContext = useContext(DocumentTreeContext);
    const { platformPrivileges } = useUserContext();
    const config = useAppConfig();
    const launchDocumentIngestionSource = useLaunchDocumentIngestionSource();

    const canImportFromScheduledSources = Boolean(
        platformPrivileges?.manageIngestion && config.featureFlags.showIngestionPageRedesign,
    );

    const [step, setStep] = useState<ImportStep>('source');
    const [sourceType, setSourceType] = useState<ImportSourceType | null>(null);
    const [parentDocumentUrn, setParentDocumentUrn] = useState<string | null | undefined>(defaultParentUrn);
    const [uploadFiles, setUploadFiles] = useState<File[]>([]);
    const { importFiles, error: fileError, result: fileResult } = useImportFromFiles();

    useEffect(() => {
        if (visible) {
            setParentDocumentUrn(defaultParentUrn);
        }
    }, [visible, defaultParentUrn]);

    const handleClose = useCallback(() => {
        setStep('source');
        setSourceType(null);
        setUploadFiles([]);
        setParentDocumentUrn(defaultParentUrn);
        onClose();
    }, [onClose, defaultParentUrn]);

    const handleSelectSource = useCallback((source: ImportSourceType) => {
        setSourceType(source);
        setStep('configure');
    }, []);

    const handleImport = useCallback(async () => {
        setStep('importing');
        try {
            const importResult = await importFiles(uploadFiles, true, useCase, parentDocumentUrn ?? null);
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
            onSuccess?.(parentDocumentUrn ?? null);
        } catch {
            setStep('result');
        }
    }, [sourceType, importFiles, uploadFiles, useCase, onSuccess, parentDocumentUrn]);

    const handleContinueToIngestionSetup = useCallback(() => {
        if (!sourceType || sourceType === ImportSourceType.FILE_UPLOAD) {
            return;
        }
        analytics.event({
            type: EventType.ImportDocumentsEvent,
            source: sourceType,
            createdCount: 0,
            updatedCount: 0,
            failedCount: 0,
        });
        launchDocumentIngestionSource({
            source: sourceType,
            parentDocumentUrn: parentDocumentUrn ?? null,
        });
        handleClose();
    }, [sourceType, launchDocumentIngestionSource, parentDocumentUrn, handleClose]);

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
        onContinueToIngestionSetup: handleContinueToIngestionSetup,
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
            <DocumentTreeContext.Provider value={documentTreeContext!}>
                <Content>
                    {step === 'source' && (
                        <ImportDocumentsSourceGrid
                            canImportFromScheduledSources={canImportFromScheduledSources}
                            onSelectSource={handleSelectSource}
                        />
                    )}
                    {step === 'configure' && sourceType && (
                        <ImportDocumentsConfigureStep
                            sourceType={sourceType}
                            uploadFiles={uploadFiles}
                            onFilesChange={setUploadFiles}
                            parentDocumentUrn={parentDocumentUrn ?? null}
                            onSelectParent={setParentDocumentUrn}
                            getNode={documentTreeContext?.getNode}
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
            </DocumentTreeContext.Provider>
        </Modal>
    );
}
