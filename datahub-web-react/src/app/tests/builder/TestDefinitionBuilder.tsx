import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { YamlEditor } from '../../ingest/source/builder/YamlEditor';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 16px;
`;

const BorderedSection = styled(Section)`
    border: solid ${ANTD_GRAY[4]} 0.5px;
`;

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
`;

type Props = {
    initialValue?: string | null;
    onChange: (value: string) => void;
};

/**
 * The step for defining a test definition
 */
export const TestDefinitionForm = ({ initialValue, onChange }: Props) => {
    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>Define your Test</SelectTemplateHeader>
                <Typography.Text>
                    For more information about how to configure a Test, check out the{' '}
                    <a
                        href="https://docs.acryl.io/-Mhxve3SFaX4GN0xKMZB/administering-datahub/metadata-tests d"
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        DataHub Tests Guide.
                    </a>
                </Typography.Text>
            </Section>
            <BorderedSection>
                <YamlEditor initialText={initialValue || ''} height="260px" onChange={onChange} />
            </BorderedSection>
        </>
    );
};
