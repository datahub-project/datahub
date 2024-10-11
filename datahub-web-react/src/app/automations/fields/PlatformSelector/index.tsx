import React from 'react';
import { Select, Alert } from 'antd';
import styled from 'styled-components';

import type { DataPlatform } from '@types';
import type { ComponentBaseProps } from '@app/automations/types';

import { useGetDataPlatformsQuery } from '@src/graphql/dataPlatform.generated';

import { ContainerSelector } from '@app/automations/fields/ContainerSelector';
import { PreviewImage } from '@src/app/entity/shared/containers/profile/header/PlatformContent/PlatformContentView';
import { StepHeader } from '../components';

// State Type (ensures the state is correctly applied across templates)
export type PlatformSelectorStateType = {
    platforms: string[];
    containers: string[];
};
const StyledOption = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

// Component
export const PlatformSelector = ({ state, props, passStateToParent }: ComponentBaseProps) => {
    // Defined in @app/automations/fields/index
    const { platforms = [], containers = [] } = state as PlatformSelectorStateType;

    // Defined in @app/automations/fields/index
    const { platformUrns, enableContainerSelection } = props;

    // Get selectables platforms
    const { data, loading } = useGetDataPlatformsQuery({
        variables: {
            urns: platformUrns,
        },
        fetchPolicy: 'no-cache',
    });

    const dataPlatforms = (data?.entities as DataPlatform[]) || ([] as DataPlatform[]);

    return (
        <>
            <Select
                value={platforms || []}
                loading={loading}
                mode="multiple"
                placeholder="Select platforms…"
                onSelect={(platformUrn: string) => {
                    passStateToParent({ platforms: [...platforms, platformUrn] });
                }}
                onDeselect={(platformUrn: string) => {
                    passStateToParent({ platforms: platforms.filter((p) => platformUrn !== p) });
                }}
            >
                {dataPlatforms.map((platform) => (
                    <Select.Option value={platform.urn} key={platform.urn}>
                        <StyledOption>
                            {platform.properties?.logoUrl ? (
                                <PreviewImage
                                    preview={false}
                                    src={platform.properties?.logoUrl}
                                    placeholder
                                    alt={platform?.name}
                                />
                            ) : null}
                            <span>{platform.name}</span>
                        </StyledOption>
                    </Select.Option>
                ))}
            </Select>
            {enableContainerSelection && platforms.length > 0 && (
                <div>
                    <StepHeader>
                        <p style={{ marginBottom: '0.25em', marginTop: '0.75em' }}>
                            Now choose the containers for these platforms.
                        </p>
                    </StepHeader>
                    <ContainerSelector
                        state={{ containers }}
                        props={{ platforms }}
                        passStateToParent={(value) => passStateToParent({ platforms, ...value })}
                    />
                </div>
            )}
            <Alert
                type="warning"
                message="A maximum of 10k assets can be classified using a single automation. Use optional filters to narrow down your selection set!"
                style={{ marginTop: '0.5em' }}
            />
        </>
    );
};
