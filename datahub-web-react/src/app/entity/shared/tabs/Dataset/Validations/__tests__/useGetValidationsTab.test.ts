import { useGetValidationsTab } from '../useGetValidationsTab';

describe('useGetValidationsTab', () => {
    it('should correctly extract valid tab', () => {
        const pathname = '/dataset/urn:li:abc/Validation/Assertions';
        const tabNames = ['Assertions'];
        const res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toEqual('Assertions');
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Validation');
    });
    it('should extract undefined for invalid tab', () => {
        const pathname = '/dataset/urn:li:abc/Validation/Assertions';
        const tabNames = ['Tests'];
        const res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toBeUndefined();
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Validation');
    });
    it('should extract undefined for missing tab', () => {
        const pathname = '/dataset/urn:li:abc/Validation';
        const tabNames = ['Tests'];
        const res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toBeUndefined();
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Validation');
    });
    it('should handle trailing slashes', () => {
        let pathname = '/dataset/urn:li:abc/Validation/Assertions/';
        let tabNames = ['Assertions'];
        let res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toEqual('Assertions');
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Validation');

        pathname = '/dataset/urn:li:abc/Validation/';
        tabNames = ['Assertions'];
        res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toBeUndefined();
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Validation');
    });
});
