import { Input, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import type { GitHubImportConfig } from '@app/context/import/hooks/useImportFromGitHub';

const FormGrid = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const FormField = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const Row = styled.div`
    display: flex;
    gap: 12px;

    > * {
        flex: 1;
    }
`;

type GitHubImportSourceProps = {
    config: GitHubImportConfig;
    onChange: (config: GitHubImportConfig) => void;
};

export default function GitHubImportSource({ config, onChange }: GitHubImportSourceProps) {
    const update = (partial: Partial<GitHubImportConfig>) => onChange({ ...config, ...partial });

    return (
        <FormGrid>
            <FormField>
                <Text weight="semiBold" size="sm">
                    Repository
                </Text>
                <Input
                    placeholder="owner/repo or https://github.com/owner/repo"
                    value={config.repoUrl}
                    setValue={(val) => update({ repoUrl: val })}
                />
            </FormField>

            <Row>
                <FormField>
                    <Text weight="semiBold" size="sm">
                        Branch
                    </Text>
                    <Input placeholder="main" value={config.branch} setValue={(val) => update({ branch: val })} />
                </FormField>
                <FormField>
                    <Text weight="semiBold" size="sm">
                        Path (optional)
                    </Text>
                    <Input placeholder="docs/" value={config.path} setValue={(val) => update({ path: val })} />
                </FormField>
            </Row>

            <FormField>
                <Text weight="semiBold" size="sm">
                    File Extensions
                </Text>
                <Input
                    placeholder=".md, .txt"
                    value={config.fileExtensions.join(', ')}
                    setValue={(val) =>
                        update({
                            fileExtensions: val
                                .split(',')
                                .map((s) => s.trim())
                                .filter(Boolean),
                        })
                    }
                />
                <Text color="gray" colorLevel={1700} size="xs">
                    Comma-separated list of extensions to include
                </Text>
            </FormField>

            <FormField>
                <Text weight="semiBold" size="sm">
                    Personal Access Token
                </Text>
                <Input
                    isPassword
                    placeholder="ghp_xxxxxxxxxxxx"
                    value={config.githubToken}
                    setValue={(val) => update({ githubToken: val })}
                />
                <Text color="gray" colorLevel={1700} size="xs">
                    Token must have &quot;Contents&quot; read permission for the repository. Not stored.
                </Text>
            </FormField>
        </FormGrid>
    );
}
