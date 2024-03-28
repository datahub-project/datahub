import React, { useEffect } from 'react';
import styled from 'styled-components/macro';
import { useEntityRegistry } from '../useEntityRegistry';
import { ListBusinessAttributesQuery, useListBusinessAttributesQuery } from '../../graphql/businessAttribute.generated';
import { sortBusinessAttributes } from './businessAttributeUtils';
import AttributeItem from './AttributeItem';

const BrowserWrapper = styled.div`
    color: #262626;
    font-size: 12px;
    max-height: calc(100% - 47px);
    padding: 10px 20px 20px 20px;
    overflow: auto;
`;

interface Props {
    isSelecting?: boolean;
    hideTerms?: boolean;
    refreshBrowser?: boolean;
    selectAttribute?: (urn: string, displayName: string) => void;
    attributeData?: ListBusinessAttributesQuery;
}

function AttributeBrowser(props: Props) {
    const { isSelecting, hideTerms, refreshBrowser, selectAttribute, attributeData } = props;

    const { refetch: refetchAttributes } = useListBusinessAttributesQuery({
        variables: {
            start: 0,
            count: 10,
            query: '*',
        },
    });

    const displayedAttributes = attributeData?.listBusinessAttributes?.businessAttributes || [];

    const entityRegistry = useEntityRegistry();
    const sortedAttributes = displayedAttributes.sort((termA, termB) =>
        sortBusinessAttributes(entityRegistry, termA, termB),
    );

    useEffect(() => {
        if (refreshBrowser) {
            refetchAttributes();
        }
    }, [refreshBrowser, refetchAttributes]);

    return (
        <BrowserWrapper>
            {!hideTerms &&
                sortedAttributes.map((attribute) => (
                    <AttributeItem
                        key={attribute.urn}
                        attribute={attribute}
                        isSelecting={isSelecting}
                        selectAttribute={selectAttribute}
                    />
                ))}
        </BrowserWrapper>
    );
}

export default AttributeBrowser;
