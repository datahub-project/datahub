import { GlobalOutlined } from '@ant-design/icons';
import { Modal, Tag, message } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';

import { useHasMatchedFieldByUrn } from '@app/search/context/SearchResultContext';
import { MatchedFieldName } from '@app/search/matches/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useRemoveBusinessAttributeMutation } from '@graphql/mutations.generated';
import { BusinessAttributeAssociation, EntityType } from '@types';

const highlightMatchStyle = { background: '#ffe58f', padding: '0' };

const StyledAttribute = styled(Tag)<{ fontSize?: number; highlightAttribute?: boolean }>`
    &&& {
        ${(props) =>
            props.highlightAttribute &&
            `background: ${props.theme.styles['highlight-color']};
            border: 1px solid ${props.theme.styles['highlight-border-color']};
        `}
    }
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
`;

interface Props {
    businessAttribute: BusinessAttributeAssociation | undefined;
    entityUrn?: string;
    canRemove?: boolean;
    readOnly?: boolean;
    highlightText?: string;
    fontSize?: number;
    onOpenModal?: () => void;
    refetch?: () => Promise<any>;
}

export default function AttributeContent({
    businessAttribute,
    canRemove,
    readOnly,
    highlightText,
    fontSize,
    onOpenModal,
    entityUrn,
    refetch,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const [removeBusinessAttributeMutation] = useRemoveBusinessAttributeMutation();
    const highlightAttribute = useHasMatchedFieldByUrn(
        businessAttribute?.businessAttribute?.urn || '',
        'businessAttributes' as MatchedFieldName,
    );

    const removeAttribute = (attributeToRemove: BusinessAttributeAssociation) => {
        onOpenModal?.();
        const AttributeName =
            attributeToRemove &&
            entityRegistry.getDisplayName(
                attributeToRemove.businessAttribute.type,
                attributeToRemove.businessAttribute,
            );
        Modal.confirm({
            title: `Do you want to remove ${AttributeName} attribute?`,
            content: `Are you sure you want to remove the ${AttributeName} attribute?`,
            onOk() {
                if (attributeToRemove.associatedUrn || entityUrn) {
                    removeBusinessAttributeMutation({
                        variables: {
                            input: {
                                businessAttributeUrn: attributeToRemove.businessAttribute.urn,
                                resourceUrn: [
                                    {
                                        resourceUrn: attributeToRemove.associatedUrn || entityUrn || '',
                                        subResource: null,
                                        subResourceType: null,
                                    },
                                ],
                            },
                        },
                    })
                        .then(({ errors }) => {
                            if (!errors) {
                                message.success({ content: 'Removed Business Attribute!', duration: 2 });
                            }
                        })
                        .then(refetch)
                        .catch((e) => {
                            message.destroy();
                            message.error({
                                content: `Failed to remove business attribute: \n ${e.message || ''}`,
                                duration: 3,
                            });
                        });
                }
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <StyledAttribute
            style={{ cursor: 'pointer', whiteSpace: 'normal' }}
            closable={canRemove && !readOnly}
            onClose={(e) => {
                e.preventDefault();
                removeAttribute(businessAttribute as BusinessAttributeAssociation);
            }}
            fontSize={fontSize}
            highlightAttribute={highlightAttribute}
        >
            <GlobalOutlined style={{ marginRight: '4px' }} />
            <Highlight style={{ marginLeft: 0 }} matchStyle={highlightMatchStyle} search={highlightText}>
                {entityRegistry.getDisplayName(EntityType.BusinessAttribute, businessAttribute?.businessAttribute)}
            </Highlight>
        </StyledAttribute>
    );
}
