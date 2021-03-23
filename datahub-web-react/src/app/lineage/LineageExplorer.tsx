import React from 'react';
import { useParams } from 'react-router';

type LineageExplorerParams = {
    type?: string;
    urn?: string;
};

export default function LineageExplorer() {
    const { type, urn } = useParams<LineageExplorerParams>();

    return (
        <span>
            {type}
            {urn}
        </span>
    );
}
