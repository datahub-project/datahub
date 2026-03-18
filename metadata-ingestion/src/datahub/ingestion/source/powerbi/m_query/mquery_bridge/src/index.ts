import * as readline from "readline";
import { TaskUtils, DefaultSettings, ResultKind } from "@microsoft/powerquery-parser";

const rl = readline.createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
});

rl.on("line", (line: string) => {
    let req: { text: string };
    try {
        req = JSON.parse(line) as { text: string };
    } catch (e) {
        write({ ok: false, error: `JSON parse error: ${String(e)}` });
        return;
    }

    const { text } = req;
    if (typeof text !== "string") {
        write({ ok: false, error: "Invalid request: 'text' field must be a string" });
        return;
    }

    TaskUtils.tryLexParse(DefaultSettings, text)
        .then((result) => {
            if (result.resultKind === ResultKind.Error) {
                const err = result.error;
                const errorMsg =
                    typeof err === "object" && err !== null
                        ? `${(err as any).kind ?? "ParseError"}: ${(err as any).message ?? String(err)}`
                        : String(err);
                write({ ok: false, error: errorMsg });
            } else {
                const nodeIdMap = [
                    ...result.nodeIdMapCollection.astNodeById.entries(),
                ];
                write({ ok: true, nodeIdMap });
            }
        })
        .catch((e) => {
            write({ ok: false, error: String(e) });
        });
});

rl.on("close", () => {
    process.exit(0);
});

function write(obj: object): void {
    process.stdout.write(JSON.stringify(obj) + "\n");
}
