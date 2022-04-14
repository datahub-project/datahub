export function WhereAmI() {
    const currUrl = window.location.href;
    // this wacky setup is because the URL is different when running docker-compose vs Ingress
    // for docker-compose, need to change port. For ingress, just modify subpath will do.
    // having a setup that works for both makes development easier.
    // for UI edit pages, the URL is complicated, need to find the root path.
    const mainPathLength = currUrl.split('/', 3).join('/').length;
    const mainPath = `${currUrl.substring(0, mainPathLength + 1)}`;
    let publishUrl = mainPath.includes(':3000') ? mainPath.replace(':3000/', ':8001/') : mainPath;
    publishUrl = publishUrl.includes(':9002') ? publishUrl.replace(':9002/', ':8001/') : publishUrl;
    return publishUrl;
}
