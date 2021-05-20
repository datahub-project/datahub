import { render } from '@testing-library/react';
import React from 'react';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import RelatedEntity from '../RelatedEntity';

const searchResult = {
    DATASET: [
        {
            __typename: 'Dataset',
            urn: 'urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)',
            type: 'DATASET',
            name: 'SampleKafkaDataset',
            origin: 'PROD',
            description: '',
            uri: null,
            platform: {
                __typename: 'DataPlatform',
                name: 'kafka',
                info: {
                    __typename: 'DataPlatformInfo',
                    logoUrl:
                        'https://raw.githubusercontent.com/linkedin/datahub/master/datahub-web-react/src/images/kafkalogo.png',
                },
            },
            platformNativeType: null,
            tags: [],
            properties: null,
            ownership: {
                __typename: 'Ownership',
                owners: [
                    {
                        __typename: 'Owner',
                        owner: {
                            __typename: 'CorpUser',
                            urn: 'urn:li:corpuser:shtr',
                            type: 'CORP_USER',
                            username: 'shtr',
                            info: {
                                __typename: 'CorpUserInfo',
                                active: true,
                                displayName: 'shtr',
                                title: null,
                                firstName: null,
                                lastName: null,
                                fullName: null,
                            },
                            editableInfo: null,
                        },
                        type: 'DATAOWNER',
                    },
                ],
                lastModified: {
                    __typename: 'AuditStamp',
                    time: 1581407189000,
                },
            },
            globalTags: null,
        },
    ],
    GLOSSARY_TERM: [
        {
            __typename: 'GlossaryTerm',
            urn: 'urn:li:glossaryTerm:instruments.FinancialInstrument_v2',
            type: 'GLOSSARY_TERM',
            name: 'FinancialInstrument_v2',
            glossaryTermInfo: {
                __typename: 'GlossaryTermInfo',
                definition:
                    'written contract that gives rise to both a financial asset of one entity and a financial liability of another entity',
                termSource: 'FIBO',
                sourceRef: 'sourceRef',
                sourceURI:
                    'https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/FinancialInstrument',
                customProperties: [
                    {
                        __typename: 'StringMapEntry',
                        key: 'FQDN',
                        value: 'full',
                    },
                ],
            },
        },
    ],
};

describe('Preview', () => {
    it('renders', () => {
        const { getByText } = render(
            <TestPageContainer>
                <RelatedEntity entityPath="dataset" searchResult={searchResult} />
            </TestPageContainer>,
        );
        expect(getByText('SampleKafkaDataset')).toBeInTheDocument();
    });
});
