/**
 * Microbench for Schema tab hot paths: groupByFieldPath parent lookup,
 * editableSchemaFieldInfo scans, and expanded-row className prefix checks.
 *
 * Run: node datahub-web-react/scripts/schema-tab-perf-bench.mjs
 */

const UNION_TOKEN = '[type=union]';
const KEY_SCHEMA_PREFIX = '[key=True].';
const VERSION_PREFIX = '[version=2.0].';

function downgradeV2FieldPath(fieldPath) {
    if (!fieldPath) return fieldPath;
    const cleanedFieldPath = fieldPath.replace(KEY_SCHEMA_PREFIX, '').replace(VERSION_PREFIX, '');
    return cleanedFieldPath
        .split('.')
        .map((segment) => {
            if (segment.startsWith('[') && segment.endsWith(']')) return null;
            return segment.replace(/\[[^\]]*\]/g, '');
        })
        .filter(Boolean)
        .join('.');
}

function pathMatchesExact(a, b) {
    return a === b;
}

function pathMatchesInsensitiveToV2(fieldPathA, fieldPathB) {
    if (!fieldPathA || !fieldPathB) return false;
    if (fieldPathA === fieldPathB) return true;
    const a = downgradeV2FieldPath(fieldPathA);
    const b = downgradeV2FieldPath(fieldPathB);
    return !!a && !!b && a.toLowerCase() === b.toLowerCase();
}

function makeNestedSchema(topLevel, depth, breadth) {
    const fields = [];
    function walk(prefix, d) {
        for (let i = 0; i < breadth; i++) {
            const name = `f${i}`;
            const path =
                d === 0
                    ? `[version=2.0].[type=struct].${name}`
                    : `${prefix}.[type=struct].${name}`;
            fields.push({ fieldPath: path, nullable: true, recursive: false, type: 'STRING' });
            if (d + 1 < depth) walk(path, d + 1);
        }
    }
    for (let t = 0; t < topLevel; t++) {
        const root = `[version=2.0].[type=struct].root${t}`;
        fields.push({ fieldPath: root, nullable: true, recursive: false, type: 'STRUCT' });
        walk(root, 1);
    }
    // Add a few union-shaped paths
    fields.push({
        fieldPath: '[version=2.0].[type=union].payload.VariantA.inner',
        nullable: true,
        recursive: false,
        type: 'STRING',
    });
    fields.push({
        fieldPath: '[version=2.0].[type=union].payload',
        nullable: true,
        recursive: false,
        type: 'UNION',
    });
    return fields;
}

function filterKeyFieldPath(showKeySchema, field) {
    if (showKeySchema === undefined) return true;
    return field.fieldPath.indexOf(KEY_SCHEMA_PREFIX) > -1 ? showKeySchema : !showKeySchema;
}

function groupByFieldPathOld(schemaRows, options = { showKeySchema: false }) {
    const rows = [...(schemaRows?.filter(filterKeyFieldPath.bind({}, options.showKeySchema)) || [])];
    const outputRows = [];
    const outputRowByPath = {};

    for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
        let parentRow = null;
        const row = { children: undefined, ...rows[rowIndex], depth: 0 };

        for (let j = rowIndex - 1; j >= 0; j--) {
            const rowTokens = row.fieldPath.split('.');
            const isQualifyingUnionField = rowTokens[rowTokens.length - 3] === UNION_TOKEN;
            if (isQualifyingUnionField) {
                rowTokens.splice(rowTokens.length - 2, 1);
                const parentPath = rowTokens.join('.');
                if (rows[j].fieldPath === parentPath) {
                    parentRow = outputRowByPath[rows[j].fieldPath];
                    break;
                }
            } else {
                let parentPath = null;
                for (let lastParentTokenIndex = rowTokens.length - 2; lastParentTokenIndex >= 0; --lastParentTokenIndex) {
                    const lastParentToken = rowTokens[lastParentTokenIndex];
                    if (lastParentToken && lastParentToken[0] !== '[') {
                        parentPath = rowTokens.slice(0, lastParentTokenIndex + 1).join('.');
                        break;
                    }
                }
                if (parentPath && rows[j].fieldPath === parentPath) {
                    parentRow = outputRowByPath[rows[j].fieldPath];
                    break;
                }
            }
        }

        if (parentRow) {
            row.depth = (parentRow.depth || 0) + 1;
            row.parent = parentRow;
            parentRow.children = [...(parentRow.children || []), row];
        } else {
            outputRows.push(row);
        }
        outputRowByPath[row.fieldPath] = row;
    }
    return outputRows;
}

function getParentPath(fieldPath) {
    const tokens = fieldPath.split('.');
    const isQualifyingUnionField = tokens[tokens.length - 3] === UNION_TOKEN;

    if (isQualifyingUnionField) {
        const parentTokens = [...tokens];
        parentTokens.splice(parentTokens.length - 2, 1);
        return parentTokens.join('.');
    }

    for (let i = tokens.length - 2; i >= 0; i--) {
        if (tokens[i] && tokens[i][0] !== '[') {
            return tokens.slice(0, i + 1).join('.');
        }
    }
    return null;
}

function groupByFieldPathNew(schemaRows, options = { showKeySchema: false }) {
    const rows = [...(schemaRows?.filter(filterKeyFieldPath.bind({}, options.showKeySchema)) || [])];
    const outputRows = [];
    const outputRowByPath = {};

    for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
        const row = { children: undefined, ...rows[rowIndex], depth: 0 };
        const parentPath = getParentPath(row.fieldPath);
        const parentRow = parentPath ? (outputRowByPath[parentPath] ?? null) : null;

        if (parentRow) {
            row.depth = (parentRow.depth || 0) + 1;
            row.parent = parentRow;
            parentRow.children = [...(parentRow.children || []), row];
        } else {
            outputRows.push(row);
        }
        outputRowByPath[row.fieldPath] = row;
    }
    return outputRows;
}

function extractOld(editableInfos, recordPath) {
    const editableFieldInfo = editableInfos.find((c) => pathMatchesExact(c.fieldPath, recordPath));
    const extra = editableInfos
        .filter((c) => pathMatchesInsensitiveToV2(c.fieldPath, recordPath))
        .flatMap((info) => info.tags || []);
    return { editableFieldInfo, extraCount: extra.length };
}

function buildMaps(editableInfos) {
    const exactMap = new Map();
    const v2NormalizedMap = new Map();
    for (const info of editableInfos) {
        if (!exactMap.has(info.fieldPath)) exactMap.set(info.fieldPath, info);
        const normalizedPath = (downgradeV2FieldPath(info.fieldPath) ?? info.fieldPath).toLowerCase();
        if (!v2NormalizedMap.has(normalizedPath)) v2NormalizedMap.set(normalizedPath, []);
        v2NormalizedMap.get(normalizedPath).push(info);
    }
    return { exactMap, v2NormalizedMap };
}

function extractNew(maps, recordPath) {
    const editableFieldInfo = maps.exactMap.get(recordPath);
    const normalizedRecordPath = (downgradeV2FieldPath(recordPath) ?? recordPath).toLowerCase();
    const extra = (maps.v2NormalizedMap.get(normalizedRecordPath) ?? []).flatMap((info) => info.tags || []);
    return { editableFieldInfo, extraCount: extra.length };
}

function rowClassNameOld(path, expandedRows) {
    let className = '';
    expandedRows.forEach((row) => {
        if (path.startsWith(`${row}.`)) className += ' expanded-child';
    });
    return className;
}

function rowClassNameNew(path, expandedRowPrefixes) {
    let className = '';
    if (expandedRowPrefixes.some((prefix) => path.startsWith(prefix))) className += ' expanded-child';
    return className;
}

function time(fn, iters) {
    const start = performance.now();
    let sink = 0;
    for (let i = 0; i < iters; i++) sink += fn(i) ? 1 : 0;
    return { ms: performance.now() - start, sink };
}

function assertSameGroup(a, b) {
    const flatten = (rows, acc = []) => {
        for (const r of rows) {
            acc.push(`${r.fieldPath}:${r.depth}`);
            if (r.children) flatten(r.children, acc);
        }
        return acc;
    };
    const fa = flatten(a).sort().join('|');
    const fb = flatten(b).sort().join('|');
    if (fa !== fb) throw new Error('groupByFieldPath old/new mismatch');
}

const fields = makeNestedSchema(40, 4, 3);
console.log(`Synthetic schema fields: ${fields.length}`);

const groupedOld = groupByFieldPathOld(fields);
const groupedNew = groupByFieldPathNew(fields);
assertSameGroup(groupedOld, groupedNew);

const groupIters = 30;
const oldGroup = time(() => groupByFieldPathOld(fields), groupIters);
const newGroup = time(() => groupByFieldPathNew(fields), groupIters);

const editableInfos = fields.map((f, i) => ({
    fieldPath: i % 5 === 0 ? downgradeV2FieldPath(f.fieldPath) : f.fieldPath,
    tags: [{ urn: `urn:li:tag:t${i % 20}` }],
}));
// Mix in camelCase / lowercase pair (ING-2174 shape)
editableInfos.push({
    fieldPath: 'payload.additionalInfo.rawCounterpartyId',
    tags: [{ urn: 'urn:li:tag:case' }],
});
const caseRecordPath =
    '[version=2.0].[type=struct].payload.[type=struct].additionalinfo.[type=string].rawcounterpartyid';

const maps = buildMaps(editableInfos);
const samplePaths = fields.filter((_, i) => i % 7 === 0).map((f) => f.fieldPath);
samplePaths.push(caseRecordPath);

let oldExtra = 0;
let newExtra = 0;
const extractIters = 20;
const oldExtract = time(() => {
    for (const p of samplePaths) {
        const r = extractOld(editableInfos, p);
        oldExtra += r.extraCount;
    }
}, extractIters);
const newExtract = time(() => {
    for (const p of samplePaths) {
        const r = extractNew(maps, p);
        newExtra += r.extraCount;
    }
}, extractIters);

const caseOld = extractOld(editableInfos, caseRecordPath);
const caseNew = extractNew(maps, caseRecordPath);
if (caseOld.extraCount === 0 || caseNew.extraCount === 0) {
    throw new Error('case-insensitive match failed in bench');
}

const expandedRows = new Set(fields.filter((_, i) => i % 11 === 0).map((f) => f.fieldPath));
const prefixes = Array.from(expandedRows).map((r) => `${r}.`);
const rowPaths = fields.map((f) => f.fieldPath);
const classIters = 40;
const oldClass = time(() => {
    for (const p of rowPaths) rowClassNameOld(p, expandedRows);
}, classIters);
const newClass = time(() => {
    for (const p of rowPaths) rowClassNameNew(p, prefixes);
}, classIters);

function fmt(label, oldMs, newMs) {
    const speedup = oldMs / newMs;
    return `${label}: old=${oldMs.toFixed(1)}ms new=${newMs.toFixed(1)}ms speedup=${speedup.toFixed(2)}x`;
}

console.log('--- Schema tab hot-path microbench ---');
console.log(fmt(`groupByFieldPath x${groupIters}`, oldGroup.ms, newGroup.ms));
console.log(
    fmt(
        `editable extract (${samplePaths.length} paths) x${extractIters}`,
        oldExtract.ms,
        newExtract.ms,
    ),
);
console.log(fmt(`rowClassName expanded-child x${classIters}`, oldClass.ms, newClass.ms));
console.log(
    JSON.stringify(
        {
            fieldCount: fields.length,
            editableCount: editableInfos.length,
            samplePaths: samplePaths.length,
            expandedCount: expandedRows.size,
            groupByFieldPath: { oldMs: oldGroup.ms, newMs: newGroup.ms, iters: groupIters },
            editableExtract: { oldMs: oldExtract.ms, newMs: newExtract.ms, iters: extractIters },
            rowClassName: { oldMs: oldClass.ms, newMs: newClass.ms, iters: classIters },
        },
        null,
        2,
    ),
);
