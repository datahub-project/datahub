import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { Modal, message } from 'antd';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '../../../../constants';
import SetDataProductModal from './SetDataProductModal';
import { DataProductLink } from '../../../../../../sharedV2/tags/DataProductLink';
import { useBatchSetDataProductMutation } from '../../../../../../../graphql/dataProduct.generated';
import { DataProduct } from '../../../../../../../types.generated';
import { SidebarSection } from '../SidebarSection';
import SectionActionButton from '../SectionActionButton';
import EmptySectionText from '../EmptySectionText';

const Content = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
    text-wrap: wrap;
`;
interface Props {
    readOnly?: boolean;
}

export default function DataProductSection({ readOnly }: Props) {
    const [isModalVisible, setIsModalVisible] = useState(false);
    const { entityData, urn } = useEntityData();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();
    const [dataProduct, setDataProduct] = useState<DataProduct | null>(null);
    const dataProductRelationships = entityData?.dataProduct?.relationships;
    const siblingUrns: string[] =
        entityData?.siblingsSearch?.searchResults?.map((sibling) => sibling.entity.urn || '') || [];

    const canEditDataProducts = !!entityData?.privileges?.canEditDataProducts;

    useEffect(() => {
        if (dataProductRelationships && dataProductRelationships.length > 0) {
            setDataProduct(dataProductRelationships[0].entity as DataProduct);
        }
    }, [dataProductRelationships]);

    function removeDataProduct() {
        batchSetDataProductMutation({ variables: { input: { resourceUrns: [urn, ...siblingUrns] } } })
            .then(() => {
                message.success({ content: 'Removed Data Product.', duration: 2 });
                setDataProduct(null);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: `Failed to remove data product. An unknown error occurred.`,
                        duration: 3,
                    });
                }
            });
    }

    const onRemoveDataProduct = () => {
        Modal.confirm({
            title: `Confirm Data Product Removal`,
            content: `Are you sure you want to remove this data product?`,
            onOk() {
                removeDataProduct();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <>
            <SidebarSection
                title="Data Product"
                content={
                    <Content>
                        {dataProduct && (
                            <DataProductLink
                                dataProduct={dataProduct}
                                closable={!readOnly}
                                readOnly={readOnly}
                                onClose={(e) => {
                                    e.preventDefault();
                                    onRemoveDataProduct();
                                }}
                                fontSize={12}
                            />
                        )}
                        {!dataProduct && <EmptySectionText message={EMPTY_MESSAGES.dataProduct.title} />}
                    </Content>
                }
                extra={
                    <SectionActionButton
                        button={dataProduct ? <EditOutlinedIcon /> : <AddRoundedIcon />}
                        onClick={(event) => {
                            setIsModalVisible(true);
                            event.stopPropagation();
                        }}
                        actionPrivilege={canEditDataProducts}
                    />
                }
            />
            {isModalVisible && (
                <SetDataProductModal
                    urns={[urn, ...siblingUrns]}
                    currentDataProduct={dataProduct || null}
                    onModalClose={() => setIsModalVisible(false)}
                    setDataProduct={setDataProduct}
                />
            )}
        </>
    );
}
