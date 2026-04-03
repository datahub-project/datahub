import { CheckCircleOutlined, ExclamationCircleOutlined, LoadingOutlined } from '@ant-design/icons';
import { Modal } from '@components';
import { GithubLogo } from '@phosphor-icons/react/dist/csr/GithubLogo';
import { Upload } from '@phosphor-icons/react/dist/csr/Upload';
import { Spin } from 'antd';
import React, { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { ModalButton } from '@components/components/Modal/Modal';

import ImportParentSelector from '@app/context/import/ImportParentSelector';
import { useImportFromFiles } from '@app/context/import/hooks/useImportFromFiles';
import type { GitHubImportConfig } from '@app/context/import/hooks/useImportFromGitHub';
import { useImportFromGitHub } from '@app/context/import/hooks/useImportFromGitHub';
import { ImportSourceType, ImportStep, ImportUseCase } from '@app/context/import/import.types';
import FileUploadSource from '@app/context/import/sources/FileUploadSource';
import GitHubImportSource from '@app/context/import/sources/GitHubImportSource';
import { DocumentTreeContext } from '@app/document/DocumentTreeContext';
import { Text } from '@src/alchemy-components';

const Content = styled.div`
    display: flex;
    flex-direction: column;
    gap: 20px;
    min-height: 200px;
`;

const SourceGrid = styled.div`
    display: grid;
    grid-template-columns: 1fr 1fr;
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

const DEFAULT_GITHUB_CONFIG: GitHubImportConfig = {
    repoUrl: '',
    branch: 'main',
    path: '',
    fileExtensions: ['.md', '.txt'],
    githubToken: '',
    showInGlobalContext: true,
};

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

    const [step, setStep] = useState<ImportStep>('source');
    const [sourceType, setSourceType] = useState<ImportSourceType | null>(null);
    const [parentDocumentUrn, setParentDocumentUrn] = useState<string | null | undefined>(defaultParentUrn);

    // Sync parent selector with current document when modal opens
    useEffect(() => {
        if (visible) {
            setParentDocumentUrn(defaultParentUrn);
        }
    }, [visible, defaultParentUrn]);

    // File upload state
    const [uploadFiles, setUploadFiles] = useState<File[]>([]);
    const { importFiles, error: fileError, result: fileResult } = useImportFromFiles();

    // GitHub state
    const [githubConfig, setGithubConfig] = useState<GitHubImportConfig>(DEFAULT_GITHUB_CONFIG);
    const { importFromGitHub, error: githubError, result: githubResult } = useImportFromGitHub();

    const result = sourceType === ImportSourceType.FILE_UPLOAD ? fileResult : githubResult;
    const importError = sourceType === ImportSourceType.FILE_UPLOAD ? fileError : githubError?.message || null;

    const handleImport = useCallback(async () => {
        setStep('importing');
        try {
            const parentUrn = parentDocumentUrn ?? null;
            if (sourceType === ImportSourceType.FILE_UPLOAD) {
                await importFiles(uploadFiles, true, useCase, parentUrn);
            } else if (sourceType === ImportSourceType.GITHUB) {
                await importFromGitHub({ ...githubConfig, showInGlobalContext: true }, useCase, parentUrn);
            }
            setStep('result');
            onSuccess?.(parentDocumentUrn ?? null);
        } catch {
            setStep('result');
        }
    }, [sourceType, importFiles, uploadFiles, useCase, importFromGitHub, githubConfig, onSuccess, parentDocumentUrn]);

    const handleClose = useCallback(() => {
        setStep('source');
        setSourceType(null);
        setUploadFiles([]);
        setGithubConfig(DEFAULT_GITHUB_CONFIG);
        setParentDocumentUrn(defaultParentUrn);
        onClose();
    }, [onClose, defaultParentUrn]);

    const canImport = useMemo(() => {
        if (sourceType === ImportSourceType.FILE_UPLOAD) return uploadFiles.length > 0;
        if (sourceType === ImportSourceType.GITHUB)
            return githubConfig.repoUrl.trim() !== '' && githubConfig.githubToken.trim() !== '';
        return false;
    }, [sourceType, uploadFiles, githubConfig]);

    const buttons: ModalButton[] = useMemo(() => {
        if (step === 'source') {
            return [{ text: 'Cancel', variant: 'outline', onClick: handleClose }];
        }
        if (step === 'configure') {
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
        // result
        return [{ text: 'Done', variant: 'filled', onClick: handleClose }];
    }, [step, handleClose, canImport, handleImport]);

    if (!visible) return null;

    const totalImported = result?.createdCount ?? 0;
    const totalUpdated = result?.updatedCount ?? 0;
    const totalFailed = result?.failedCount ?? 0;

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
                        <SourceGrid>
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
                                    Upload .md, .txt, .pdf, .docx files
                                </Text>
                            </SourceCard>
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
                                    Import from a GitHub repo
                                </Text>
                            </SourceCard>
                        </SourceGrid>
                    )}

                    {step === 'configure' && (
                        <>
                            {sourceType === ImportSourceType.FILE_UPLOAD && (
                                <FileUploadSource files={uploadFiles} onFilesChange={setUploadFiles} />
                            )}
                            {sourceType === ImportSourceType.GITHUB && (
                                <GitHubImportSource config={githubConfig} onChange={setGithubConfig} />
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
                                        {totalImported > 0 && `${totalImported} created`}
                                        {totalImported > 0 && totalUpdated > 0 && ', '}
                                        {totalUpdated > 0 && `${totalUpdated} updated`}
                                        {totalFailed > 0 && `, ${totalFailed} failed`}
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
