/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export function mergeArraysOfObjects<T>(arrayA: Array<T>, arrayB: Array<T>, keyGetter: (item: T) => any): Array<T> {
    const keysFromArrayB = arrayB.map(keyGetter);
    return [...arrayA.filter((item) => !keysFromArrayB.includes(keyGetter(item))), ...arrayB];
}
