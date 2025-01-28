import { useGetValidationsTab } from '../useGetValidationsTab';

describe('useGetValidationsTab', () => {
    it('should correctly extract valid tab', () => {
        const pathname = '/dataset/urn:li:abc/Quality/List';
        const tabNames = ['List'];
        const res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toEqual('List');
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Quality');
    });
    it('should extract undefined for invalid tab', () => {
        const pathname = '/dataset/urn:li:abc/Governance/Assertions';
        const tabNames = ['Tests'];
        const res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toBeUndefined();
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Governance');
    });
    it('should extract undefined for missing tab', () => {
        const pathname = '/dataset/urn:li:abc/Governance';
        const tabNames = ['Tests'];
        const res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toBeUndefined();
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Governance');
    });
    it('should handle trailing slashes', () => {
        let pathname = '/dataset/urn:li:abc/Quality/List/';
        let tabNames = ['List'];
        let res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toEqual('List');
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Quality');

        pathname = '/dataset/urn:li:abc/Quality/';
        tabNames = ['Assertions'];
        res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toBeUndefined();
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Quality');
    });
});
