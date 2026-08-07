import { DefaultSettings, ResultKind, Task, TaskUtils } from "@microsoft/powerquery-parser";

/**
 * LexError from @microsoft/powerquery-parser often fails `instanceof Error` (legacy prototype
 * chain), but always exposes string `.name` / `.message`. Using `String(err)` on those values
 * throws `TypeError: Function.prototype.toString requires that 'this' be a Function` in V8.
 */
function formatTaskError(err: unknown): string {
    if (typeof err === "object" && err !== null) {
        const e = err as { name?: unknown; message?: unknown };
        if (typeof e.message === "string") {
            const name = typeof e.name === "string" && e.name.length > 0 ? e.name : "Error";
            return `${name}: ${e.message}`;
        }
    }
    if (err instanceof Error) {
        return `${err.name}: ${err.message}`;
    }
    try {
        return String(err);
    } catch {
        return "Unknown error";
    }
}

// Expose parseExpression on globalThis so py_mini_racer can call it.
// Bare V8 has neither Node.js `global` nor browser `window`, only globalThis.
//
// Wire protocol (always returns a JSON string, never throws into Python):
//
//   success: { ok: true,  nodeIdMap: [[id, node], ...], parentIdById: [[id, parentId], ...] }
//   failure: { ok: false, error: "Lex: ..." | "Parse: ErrorName: message" }
//
// nodeIdMap is an array of [number, object] pairs rather than a plain object
// because JSON object keys must be strings, and converting integer keys to
// strings and back on the Python side is error-prone. The Python caller
// reconstructs dict[int, dict] from these pairs. parentIdById is emitted the
// same way.
//
// parentIdById is the parser's own child -> parent index. Child nodes are
// embedded in the AST, so the tree can only be walked downward without it;
// with it the caller can walk *outward* to resolve an identifier against its
// enclosing `let` scopes, and can identify the root soundly as the single node
// that has no parent.
//
// Example node (LetExpression root of `let Source = Sql.Database(...) in Source`):
//   [1, { kind: "LetExpression", id: 1, variableList: { kind: "ArrayWrapper", elements: [...] }, ... }]
(globalThis as Record<string, unknown>).parseExpression = async function (text: unknown): Promise<string> {
    if (typeof text !== "string") {
        return JSON.stringify({ ok: false, error: "parseExpression: 'text' must be a string" });
    }
    try {
        const result = await TaskUtils.tryLexParse(DefaultSettings, text);
        if (result.resultKind === ResultKind.Error) {
            const err = result.error;
            const stage = result.stage === Task.TaskStage.Lex ? "Lex" : "Parse";
            return JSON.stringify({ ok: false, error: `${stage}: ${formatTaskError(err)}` });
        }
        const nodeIdMap = [...result.nodeIdMapCollection.astNodeById.entries()];
        const parentIdById = [...result.nodeIdMapCollection.parentIdById.entries()];
        return JSON.stringify({ ok: true, nodeIdMap, parentIdById });
    } catch (e) {
        return JSON.stringify({ ok: false, error: String(e) });
    }
};
