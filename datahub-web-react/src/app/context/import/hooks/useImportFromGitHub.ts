import { useCallback } from 'react';

import { ImportUseCase } from '@app/context/import/import.types';

import { useImportDocumentsFromGitHubMutation, usePreviewDocumentsFromGitHubQuery } from '@graphql/document.generated';
import { DocumentImportUseCase, ImportDocumentsFromGitHubInput } from '@types';

function mapUseCase(useCase: ImportUseCase): DocumentImportUseCase {
    return useCase === ImportUseCase.SKILL ? DocumentImportUseCase.Skill : DocumentImportUseCase.ContextDocument;
}

export type GitHubImportConfig = {
    repoUrl: string;
    branch: string;
    path: string;
    fileExtensions: string[];
    githubToken: string;
    showInGlobalContext: boolean;
};

export function usePreviewFromGitHub(config: GitHubImportConfig | null, useCase: ImportUseCase) {
    const input: ImportDocumentsFromGitHubInput | undefined = config
        ? {
              repoUrl: config.repoUrl,
              githubToken: config.githubToken,
              branch: config.branch || undefined,
              path: config.path || undefined,
              fileExtensions: config.fileExtensions.length ? config.fileExtensions : undefined,
              showInGlobalContext: config.showInGlobalContext,
              useCase: mapUseCase(useCase),
          }
        : undefined;

    const { data, loading, error, refetch } = usePreviewDocumentsFromGitHubQuery({
        variables: input ? { input } : undefined,
        skip: !input,
        fetchPolicy: 'network-only',
    });

    return {
        files: data?.previewDocumentsFromGitHub?.files ?? [],
        totalCount: data?.previewDocumentsFromGitHub?.totalCount ?? 0,
        loading,
        error,
        refetch,
    };
}

export function useImportFromGitHub() {
    const [importMutation, { loading, error, data }] = useImportDocumentsFromGitHubMutation();

    const importFromGitHub = useCallback(
        async (config: GitHubImportConfig, useCase: ImportUseCase, parentDocumentUrn?: string | null) => {
            const input: ImportDocumentsFromGitHubInput = {
                repoUrl: config.repoUrl,
                githubToken: config.githubToken,
                branch: config.branch || undefined,
                path: config.path || undefined,
                fileExtensions: config.fileExtensions.length ? config.fileExtensions : undefined,
                showInGlobalContext: config.showInGlobalContext,
                useCase: mapUseCase(useCase),
                parentDocumentUrn: parentDocumentUrn ?? undefined,
            };

            const result = await importMutation({ variables: { input } });
            return result.data?.importDocumentsFromGitHub ?? null;
        },
        [importMutation],
    );

    return {
        importFromGitHub,
        loading,
        error,
        result: data?.importDocumentsFromGitHub ?? null,
    };
}
