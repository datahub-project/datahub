import { TaskUtils, DefaultSettings, ResultKind } from "@microsoft/powerquery-parser";

// Use globalThis so the function is accessible in V8 (py_mini_racer),
// which has neither Node.js `global` nor browser `window`.
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
