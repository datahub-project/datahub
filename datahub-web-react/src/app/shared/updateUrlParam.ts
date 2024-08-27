import { RouteComponentProps } from 'react-router';

export function updateUrlParam(history: RouteComponentProps['history'], key: string, value: string) {
    const url = new URL(window.location.href);
    const { searchParams } = url;
    searchParams.set(key, value);
    history.replace(url.search);
}
