import { TaskUtils, DefaultSettings, ResultKind } from "@microsoft/powerquery-parser";

// Expose parseExpression on globalThis so py_mini_racer can call it.
// Bare V8 has neither Node.js `global` nor browser `window`, only globalThis.
//
// Wire protocol (always returns a JSON string, never throws into Python):
//
//   success: { ok: true,  nodeIdMap: [[id, node], ...] }
//   failure: { ok: false, error: "ParseError: ..." }
//
// nodeIdMap is an array of [number, object] pairs rather than a plain object
// because JSON object keys must be strings, and converting integer keys to
// strings and back on the Python side is error-prone. The Python caller
// reconstructs dict[int, dict] from these pairs.
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
            const errorMsg =
                typeof err === "object" && err !== null
                    ? `${(err as { kind?: string }).kind ?? "ParseError"}: ${(err as { message?: string }).message ?? String(err)}`
                    : String(err);
            return JSON.stringify({ ok: false, error: errorMsg });
        }
        const nodeIdMap = [...result.nodeIdMapCollection.astNodeById.entries()];
        return JSON.stringify({ ok: true, nodeIdMap });
    } catch (e) {
        return JSON.stringify({ ok: false, error: String(e) });
    }
};
