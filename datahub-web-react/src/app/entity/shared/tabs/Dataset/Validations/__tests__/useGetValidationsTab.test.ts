/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useGetValidationsTab } from '@app/entity/shared/tabs/Dataset/Validations/useGetValidationsTab';

describe('useGetValidationsTab', () => {
    it('should correctly extract valid tab', () => {
        const pathname = '/dataset/urn:li:abc/Quality/List';
        const tabNames = ['List'];
        const res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toEqual('List');
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Quality');
    });
    it('should extract undefined for invalid tab', () => {
        const pathname = '/dataset/urn:li:abc/Quality/Assertions';
        const tabNames = ['Tests'];
        const res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toBeUndefined();
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Quality');
    });
    it('should extract undefined for missing tab', () => {
        const pathname = '/dataset/urn:li:abc/Quality';
        const tabNames = ['Tests'];
        const res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toBeUndefined();
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Quality');
    });
    it('should handle trailing slashes', () => {
        let pathname = '/dataset/urn:li:abc/Quality/Assertions/';
        let tabNames = ['Assertions'];
        let res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toEqual('Assertions');
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Quality');

        pathname = '/dataset/urn:li:abc/Quality/';
        tabNames = ['Assertions'];
        res = useGetValidationsTab(pathname, tabNames);
        expect(res.selectedTab).toBeUndefined();
        expect(res.basePath).toEqual('/dataset/urn:li:abc/Quality');
    });
});
