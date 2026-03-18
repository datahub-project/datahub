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
        const result = TaskUtils.tryLexParse(DefaultSettings, text);
        if (result.isError) {
            write({ ok: false, error: String(result.error.message ?? result.error) });
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

function write(obj: object): void {
    process.stdout.write(JSON.stringify(obj) + "\n");
}
