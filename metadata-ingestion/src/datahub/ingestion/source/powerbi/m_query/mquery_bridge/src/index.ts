import * as readline from "readline";
import { TaskUtils, DefaultSettings } from "@microsoft/powerquery-parser";

const rl = readline.createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
});

rl.on("line", (line: string) => {
    try {
        const req = JSON.parse(line) as { text: string };
        const { text } = req;
        // Fix 1: Runtime validation for request object
        if (typeof text !== "string") {
            write({ ok: false, error: "Invalid request: 'text' field must be a string" });
            return;
        }
        const result = TaskUtils.tryLexParse(DefaultSettings, text);
        if (result.isError) {
            // Fix 3: Improve error serialization with better context
            const err = result.error;
            const errorMsg = typeof err === 'object' && err !== null
                ? `${(err as any).kind ?? 'ParseError'}: ${(err as any).message ?? String(err)}`
                : String(err);
            write({ ok: false, error: errorMsg });
        } else {
            const nodeIdMap = [
                ...result.value.nodeIdMapCollection.astNodeById.entries(),
            ];
            write({ ok: true, nodeIdMap });
        }
    } catch (e) {
        write({ ok: false, error: String(e) });
    }
});

// Fix 2: Add readline close event handler for clean process termination
rl.on("close", () => {
    process.exit(0);
});

function write(obj: object): void {
    process.stdout.write(JSON.stringify(obj) + "\n");
}
