import { Breadcrumb } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { useGetContainerQuery } from '../../../graphql/container.generated';

interface Props {
    part: string;
    index: number;
    path: Array<string>;
    isProfilePage?: boolean;
    isBrowsable?: boolean;
    baseBrowsePath?: string;
}

export const GetContainerNameFromPath = ({ part, index, path, isProfilePage, isBrowsable, baseBrowsePath }: Props) => {
    const { data, loading, error } = useGetContainerQuery({
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
                {data?.container?.properties?.name}
            </Link>
        </Breadcrumb.Item>
    );
};
