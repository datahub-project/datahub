import { Modal, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useGetIngestionSourceQuery } from '../../graphql/ingestion.generated';
import { IngestionSource, IngestionSourceType } from '../../types.generated';
import { RecipeSourceDetails } from './RecipeSourceDetails';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const SectionHeader = styled(Typography.Text)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 4px;
    }
`;

const SectionParagraph = styled(Typography.Paragraph)`
    &&&& {
        padding: 0px;
        margin: 0px;
    }
`;

type Props = {
    urn: string;
    visible: boolean;
    onClose: () => void;
};

export const IngestionSourceDetailsModal = ({ urn, visible, onClose }: Props) => {
    const { data } = useGetIngestionSourceQuery({ variables: { urn } });
    // todo: use loading, error.

    const ingestionSource = data?.ingestionSource;
    const ingestionSourceType = ingestionSource?.sourceType;
    const ingestionSourceDisplayName = ingestionSource?.displayName;
    const ingestionSourceSchedule = ingestionSource?.schedule;

    const SourceDetailsView =
        ingestionSource && ingestionSourceType === IngestionSourceType.Recipe && RecipeSourceDetails;

    return (
        <Modal
            width={800}
            footer={null}
            title={<Typography.Text>{ingestionSourceDisplayName}</Typography.Text>}
            visible={visible}
            onCancel={onClose}
        >
            <Section>
                <SectionHeader strong>Name</SectionHeader>
                <SectionParagraph>{ingestionSourceDisplayName}</SectionParagraph>
            </Section>
            {SourceDetailsView && <SourceDetailsView source={ingestionSource as IngestionSource} />}
            {ingestionSourceSchedule && (
                <Section>
                    <SectionHeader strong>Schedule</SectionHeader>
                    <SectionParagraph>{ingestionSourceSchedule?.interval}</SectionParagraph>
                </Section>
            )}
        </Modal>
    );
};
