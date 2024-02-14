import React from 'react';
import { Collapse, Form } from 'antd';
import styled from 'styled-components';
import {
    Assertion,
    AssertionStdParameters,
    AssertionType,
    DatasetFilter,
    DatasetVolumeSourceType,
    IncrementingSegmentSpec,
    Monitor,
    VolumeAssertionInfo,
} from '../../../../../../../../../types.generated';
import { EvaluationScheduleBuilder } from '../steps/freshness/EvaluationScheduleBuilder';
import { VolumeTypeBuilder } from '../steps/volume/VolumeTypeBuilder';
import { VolumeParametersBuilder } from '../steps/volume/VolumeParametersBuilder';
import { getSelectedVolumeTypeOption, getVolumeTypeInfo } from '../steps/volume/utils';
import { VolumeSourceTypeBuilder } from '../steps/volume/VolumeSourceTypeBuilder';
import { VolumeFilterBuilder } from '../steps/volume/VolumeFilterBuilder';
import { useEntityData } from '../../../../../../EntityContext';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

type Props = {
    assertion: Assertion;
};

/**
 * This component is used to view Volume assertion details in read-only mode.
 */
export const VolumeAssertionDetails = ({ assertion }: Props) => {
    const { urn, entityData } = useEntityData();
    const volumeAssertion = assertion.info?.volumeAssertion;
    const volumeType = getSelectedVolumeTypeOption(volumeAssertion as VolumeAssertionInfo);
    const volumeInfo = getVolumeTypeInfo(volumeAssertion as VolumeAssertionInfo);
    const volumeParameters = volumeInfo?.parameters;
    const filter = volumeAssertion?.filter;
    const segment = (volumeInfo as any)?.segment;
    const monitor = (assertion as any)?.monitor?.relationships?.[0]?.entity as Monitor;
    const schedule = monitor?.info?.assertionMonitor?.assertions?.[0]?.schedule;
    const parameters = monitor?.info?.assertionMonitor?.assertions?.[0]?.parameters;
    const datasetVolumeParameters = parameters?.datasetVolumeParameters;
    const sourceType = datasetVolumeParameters?.sourceType;
    const platformUrn = entityData?.platform?.urn as string;

    return (
        <Form
            initialValues={{
                'volume-type': volumeType,
                parameters: volumeParameters,
            }}
        >
            <EvaluationScheduleBuilder
                value={schedule}
                assertionType={AssertionType.Volume}
                onChange={() => {}}
                disabled
            />
            <VolumeTypeBuilder onChange={() => {}} segment={segment as IncrementingSegmentSpec} disabled />
            <VolumeParametersBuilder
                volumeInfo={volumeAssertion as VolumeAssertionInfo}
                value={volumeParameters as AssertionStdParameters}
                onChange={() => {}}
                updateVolumeAssertion={() => {}}
                disabled
            />
            <Section>
                <Collapse>
                    <Collapse.Panel key="Advanced" header="Advanced">
                        <VolumeSourceTypeBuilder
                            entityUrn={urn}
                            platformUrn={platformUrn}
                            value={sourceType as DatasetVolumeSourceType}
                            onChange={() => {}}
                            disabled
                        />
                        <VolumeFilterBuilder
                            value={filter as DatasetFilter}
                            onChange={() => {}}
                            sourceType={sourceType as DatasetVolumeSourceType}
                            disabled
                        />
                    </Collapse.Panel>
                </Collapse>
            </Section>
        </Form>
    );
};
