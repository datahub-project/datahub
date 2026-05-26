import { CheckCircleOutlined, ExclamationCircleOutlined, LoadingOutlined } from '@ant-design/icons';
import { Modal } from '@components';
import { GithubLogo } from '@phosphor-icons/react/dist/csr/GithubLogo';
import { Upload } from '@phosphor-icons/react/dist/csr/Upload';
import { Spin } from 'antd';
import React, { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { ModalButton } from '@components/components/Modal/Modal';

import analytics, { EventType } from '@app/analytics';
import ImportParentSelector from '@app/context/import/ImportParentSelector';
import { useImportFromFiles } from '@app/context/import/hooks/useImportFromFiles';
import { useLaunchGitHubDocumentsIngestion } from '@app/context/import/hooks/useLaunchGitHubDocumentsIngestion';
import { ImportSourceType, ImportStep, ImportUseCase } from '@app/context/import/import.types';
import FileUploadSource from '@app/context/import/sources/FileUploadSource';
import { useUserContext } from '@app/context/useUserContext';
import { DocumentTreeContext } from '@app/document/DocumentTreeContext';
import { useAppConfig } from '@app/useAppConfig';
import { Text } from '@src/alchemy-components';

const Content = styled.div`
    display: flex;
    flex-direction: column;
    gap: 20px;
    min-height: 200px;
`;

const SourceGrid = styled.div<{ $columns: number }>`
    display: grid;
    grid-template-columns: repeat(${({ $columns }) => $columns}, 1fr);
    gap: 12px;
`;

const SourceCard = styled.button<{ $isSelected: boolean }>`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 12px;
    padding: 24px 16px;
    border: 2px solid ${({ $isSelected, theme }) => ($isSelected ? theme.colors.borderBrand : theme.colors.border)};
    border-radius: 8px;
    background: ${({ $isSelected, theme }) => ($isSelected ? theme.colors.bgSelected : 'transparent')};
    cursor: pointer;
    transition: all 0.15s ease;

    &:hover {
        border-color: ${({ theme }) => theme.colors.borderBrand};
    }

    &:disabled {
        cursor: not-allowed;
        opacity: 0.6;
    }
`;

const ResultContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 16px;
    padding: 20px 0;
`;

const SourceIcon = styled.div`
    color: ${({ theme }) => theme.colors.iconBrand};
`;

const SuccessIcon = styled(CheckCircleOutlined)`
    font-size: 36px;
    color: ${({ theme }) => theme.colors.iconSuccess};
`;

const WarningIcon = styled(ExclamationCircleOutlined)`
    font-size: 36px;
    color: ${({ theme }) => theme.colors.iconWarning};
`;

const ErrorIcon = styled(ExclamationCircleOutlined)`
    font-size: 36px;
    color: ${({ theme }) => theme.colors.iconError};
`;

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
    const launchGitHubDocumentsIngestion = useLaunchGitHubDocumentsIngestion();

    const showIngestV2 = config.featureFlags.showIngestionPageRedesign;
    const canImportFromGitHub = Boolean(platformPrivileges?.manageIngestion && showIngestV2);

    const [step, setStep] = useState<ImportStep>('source');
    const [sourceType, setSourceType] = useState<ImportSourceType | null>(null);
    const [parentDocumentUrn, setParentDocumentUrn] = useState<string | null | undefined>(defaultParentUrn);

    useEffect(() => {
        if (visible) {
            setParentDocumentUrn(defaultParentUrn);
        }
    }, [visible, defaultParentUrn]);

    const [uploadFiles, setUploadFiles] = useState<File[]>([]);
    const { importFiles, error: fileError, result: fileResult } = useImportFromFiles();

    const importError = fileError;
    const result = fileResult;

    const handleImport = useCallback(async () => {
        setStep('importing');
        try {
            const parentUrn = parentDocumentUrn ?? null;
            const importResult = await importFiles(uploadFiles, true, useCase, parentUrn);
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

    const handleGitHubImport = useCallback(() => {
        analytics.event({
            type: EventType.ImportDocumentsEvent,
            source: ImportSourceType.GITHUB,
            createdCount: 0,
            updatedCount: 0,
            failedCount: 0,
        });
        launchGitHubDocumentsIngestion({ parentDocumentUrn: parentDocumentUrn ?? null });
        onClose();
    }, [launchGitHubDocumentsIngestion, onClose, parentDocumentUrn]);

    const handleClose = useCallback(() => {
        setStep('source');
        setSourceType(null);
        setUploadFiles([]);
        setParentDocumentUrn(defaultParentUrn);
        onClose();
    }, [onClose, defaultParentUrn]);

    const canImport = useMemo(() => {
        if (sourceType === ImportSourceType.FILE_UPLOAD) {
            return uploadFiles.length > 0;
        }
        return false;
    }, [sourceType, uploadFiles]);

    const buttons: ModalButton[] = useMemo(() => {
        if (step === 'source') {
            return [{ text: 'Cancel', variant: 'outline', onClick: handleClose }];
        }
        if (step === 'configure') {
            if (sourceType === ImportSourceType.GITHUB) {
                return [
                    { text: 'Back', variant: 'outline', onClick: () => setStep('source') },
                    {
                        text: 'Continue to setup',
                        variant: 'filled',
                        onClick: handleGitHubImport,
                    },
                ];
            }
            return [
                { text: 'Back', variant: 'outline', onClick: () => setStep('source') },
                {
                    text: 'Import',
                    variant: 'filled',
                    onClick: handleImport,
                    disabled: !canImport,
                },
            ];
        }
        if (step === 'importing') {
            return [];
        }
        return [{ text: 'Done', variant: 'filled', onClick: handleClose }];
    }, [step, handleClose, canImport, handleImport, sourceType, handleGitHubImport]);

    if (!visible) return null;

    const totalImported = result?.createdCount ?? 0;
    const totalUpdated = result?.updatedCount ?? 0;
    const totalFailed = result?.failedCount ?? 0;
    const sourceColumnCount = canImportFromGitHub ? 2 : 1;

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
                        <SourceGrid $columns={sourceColumnCount}>
                            <SourceCard
                                type="button"
                                $isSelected={false}
                                onClick={() => {
                                    setSourceType(ImportSourceType.FILE_UPLOAD);
                                    setStep('configure');
                                }}
                            >
                                <SourceIcon>
                                    <Upload size={32} />
                                </SourceIcon>
                                <Text weight="semiBold">Upload Files</Text>
                                <Text color="gray" colorLevel={1700} size="sm">
                                    Upload .md, .txt, .docx files
                                </Text>
                            </SourceCard>
                            {canImportFromGitHub && (
                                <SourceCard
                                    type="button"
                                    $isSelected={false}
                                    onClick={() => {
                                        setSourceType(ImportSourceType.GITHUB);
                                        setStep('configure');
                                    }}
                                >
                                    <SourceIcon>
                                        <GithubLogo size={32} />
                                    </SourceIcon>
                                    <Text weight="semiBold">GitHub Repository</Text>
                                    <Text color="gray" colorLevel={1700} size="sm">
                                        Schedule imports via an ingestion source
                                    </Text>
                                </SourceCard>
                            )}
                        </SourceGrid>
                    )}

                    {step === 'configure' && (
                        <>
                            {sourceType === ImportSourceType.FILE_UPLOAD && (
                                <FileUploadSource files={uploadFiles} onFilesChange={setUploadFiles} />
                            )}
                            {sourceType === ImportSourceType.GITHUB && (
                                <Text color="gray" colorLevel={1700}>
                                    Configure a scheduled GitHub Documents ingestion source. You can choose the
                                    repository, branch, and credentials on the next screen. Imported files are created
                                    as native documents by default.
                                </Text>
                            )}
                            <ImportParentSelector
                                selectedParentUrn={parentDocumentUrn ?? null}
                                onSelectParent={setParentDocumentUrn}
                                getNode={documentTreeContext?.getNode}
                            />
                        </>
                    )}

                    {step === 'importing' && (
                        <ResultContainer>
                            <Spin indicator={<LoadingOutlined style={{ fontSize: 36 }} spin />} />
                            <Text weight="semiBold">Importing documents...</Text>
                            <Text color="gray" colorLevel={1700} size="sm">
                                This may take a moment
                            </Text>
                        </ResultContainer>
                    )}

                    {step === 'result' && (
                        <ResultContainer>
                            {importError ? (
                                <>
                                    <ErrorIcon />
                                    <Text weight="semiBold" size="lg">
                                        Import Failed
                                    </Text>
                                    <Text color="gray" colorLevel={600} style={{ textAlign: 'center' }}>
                                        {importError}
                                    </Text>
                                </>
                            ) : (
                                <>
                                    {totalFailed === 0 ? <SuccessIcon /> : <WarningIcon />}
                                    <Text weight="semiBold" size="lg">
                                        Import Complete
                                    </Text>
                                    <Text color="gray" colorLevel={600}>
                                        {totalImported > 0 &&
                                            `${totalImported} document${totalImported !== 1 ? 's' : ''} created`}
                                        {totalImported > 0 && totalUpdated > 0 && ', '}
                                        {totalUpdated > 0 &&
                                            `${totalUpdated} document${totalUpdated !== 1 ? 's' : ''} updated`}
                                        {totalFailed > 0 &&
                                            `, ${totalFailed} document${totalFailed !== 1 ? 's' : ''} failed`}
                                        {totalImported === 0 &&
                                            totalUpdated === 0 &&
                                            totalFailed === 0 &&
                                            'No documents imported'}
                                    </Text>
                                </>
                            )}
                        </ResultContainer>
                    )}
                </Content>
            </DocumentTreeContext.Provider>
        </Modal>
    );
}
