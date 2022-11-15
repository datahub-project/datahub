import { Badge } from 'antd';
import React, { useContext } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { GetDatasetQuery } from '../../../../../../../graphql/dataset.generated';
import { EntityType } from '../../../../../../../types.generated';
import { decodeSchemaField } from '../../../../../../lineage/utils/columnLineageUtils';
import CompactContext from '../../../../../../shared/CompactContext';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { ANTD_GRAY } from '../../../../constants';
import { useBaseEntity } from '../../../../EntityContext';
import { FkContext } from '../utils/selectedFkContext';

const ForeignKeyContent = styled.tr`
    position: absolute;
    display: flex;
    flex-direction: column;
    width: 100%;
    margin-top: -587px;
    box-shadow: inset 0 7px 16px -7px ${ANTD_GRAY[5]};
`;

const EntitySidePanel = styled.div`
    overflow-y: scroll;
    max-height: 545px;
    padding: 8px;
    width: 900px;
    border-right: 1px solid ${ANTD_GRAY[4]};
    background-color: white;
`;

const FieldBadge = styled(Badge)`
    margin-left: 4px;
    margin-top: 12px;
    &&& .ant-badge-count {
        background-color: ${ANTD_GRAY[1]};
        color: ${ANTD_GRAY[9]};
        border: 1px solid ${ANTD_GRAY[6]};
        font-size: 12px;
        font-weight: 400;
        height: 22px;
        cursor: pointer;
    }
`;

const ConstraintSection = styled.div`
    padding: 20px;
    padding-top: 40px;
    width: 100%;
    min-height: 100%;
    display: flex;
    justify-content: space-between;
    background-color: ${ANTD_GRAY[2]};
`;

const TableTitle = styled.span`
    font-size: 14px;
`;

const BodyContent = styled.div`
    display: flex;
    flex-direction: row;
    border-bottom: 1px solid ${ANTD_GRAY[4]};
`;

const HeaderContent = styled.div`
    padding-top: 8px;
    min-height: 40px;
    font-size: 16px;
    font-weight: 500;
    padding-left: 12px;
    border-bottom: 1px solid ${ANTD_GRAY[4]};
`;

const ForiegnKeyTd = styled.td`
    &&& {
        padding: 0;
    }
`;

const DatasetLink = styled(Link)`
    color: ${ANTD_GRAY[9]};
    font-weight: 800;
`;

const ArrowContainer = styled.div`
    margin-top: 40px;
`;

export const SchemaRow = React.forwardRef<HTMLTableRowElement>((props, ref) => {
    /* eslint-disable react/prop-types */
    const { children, ...rest } = props;
    const selectedFk = useContext(FkContext);
    const entityRegistry = useEntityRegistry();
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    return (
        <>
            <tr {...rest} ref={ref}>
                {children}
            </tr>
            {selectedFk?.fieldPath && props['data-row-key'] === selectedFk?.fieldPath && (
                <ForeignKeyContent>
                    <ForiegnKeyTd>
                        <HeaderContent>
                            Foreign Key to{' '}
                            <DatasetLink
                                to={entityRegistry.getEntityUrl(
                                    EntityType.Dataset,
                                    selectedFk?.constraint?.foreignDataset?.urn || '',
                                )}
                            >
                                {selectedFk.constraint?.foreignDataset?.name}
                            </DatasetLink>
                        </HeaderContent>
                        <BodyContent>
                            <EntitySidePanel>
                                <CompactContext.Provider value>
                                    {entityRegistry.renderProfile(
                                        EntityType.Dataset,
                                        selectedFk?.constraint?.foreignDataset?.urn || '',
                                    )}
                                </CompactContext.Provider>
                            </EntitySidePanel>
                            <ConstraintSection>
                                <div>
                                    <TableTitle>{baseEntity.dataset?.name}</TableTitle>
                                    {selectedFk?.constraint?.sourceFields?.map((field) => (
                                        <div key={field?.fieldPath}>
                                            <FieldBadge count={decodeSchemaField(field?.fieldPath || '')} />
                                        </div>
                                    ))}
                                </div>
                                <ArrowContainer>{'--->'}</ArrowContainer>
                                <div>
                                    <TableTitle>{selectedFk?.constraint?.foreignDataset?.name}</TableTitle>
                                    {selectedFk?.constraint?.foreignFields?.map((field) => (
                                        <div key={field?.fieldPath}>
                                            <FieldBadge count={decodeSchemaField(field?.fieldPath || '')} />
                                        </div>
                                    ))}
                                </div>
                            </ConstraintSection>
                        </BodyContent>
                    </ForiegnKeyTd>
                </ForeignKeyContent>
            )}
        </>
    );
});
