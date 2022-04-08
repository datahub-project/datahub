import { Breadcrumb } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { useGetDataPlatformQuery } from '../../../graphql/dataPlatform.generated';

const PreviewImage = styled.img`
    max-height: 18px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    margin-right: 4px;
`;
interface Props {
    part: string;
    index: number;
    path: Array<string>;
    isProfilePage?: boolean;
    isBrowsable?: boolean;
    baseBrowsePath?: string;
}

export const GetPlatformNameFromPath = ({ part, index, path, isProfilePage, isBrowsable, baseBrowsePath }: Props) => {
    const { data, loading, error } = useGetDataPlatformQuery({
        variables: { urn: part },
    });

    const createPartialPath = (parts: Array<string>) => {
        return parts.join('/');
    };
    if (!data || loading || error) {
        return null;
    }

    return (
        <Breadcrumb.Item key={`${part || index}`}>
            <Link
                to={
                    (isProfilePage && index === path.length - 1) || !isBrowsable
                        ? '#'
                        : `${baseBrowsePath}/${createPartialPath(path.slice(0, index + 1))}`
                }
            >
                <PreviewImage src={data?.dataPlatform?.properties?.logoUrl} alt="platform" />;
                {data?.dataPlatform?.properties?.displayName}
            </Link>
        </Breadcrumb.Item>
    );
};
