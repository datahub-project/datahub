"""Resolution rules for the layered env files (bin/_env_layers.py)."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
from _env_layers import EnvFile, EnvLayerError, resolve  # noqa: E402


class ResolveTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def write(self, rel: str, text: str) -> Path:
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def resolve(
        self,
        service: str = "gms",
        job: str | None = None,
        mode: str = "local",
        adhoc: list[EnvFile] | None = None,
        context: dict[str, str] | None = None,
        shell: dict[str, str] | None = None,
    ) -> dict[str, tuple[str, str]]:
        values = resolve(
            service,
            repo_root=self.root,
            job=job,
            mode=mode,
            adhoc=adhoc or [],
            context=context or {},
            shell=shell or {},
        )
        return {key: (v.value, v.source) for key, v in values.items()}

    def test_later_layers_and_files_win(self) -> None:
        self.write("env/common/gms.env", "A=common-gms\nB=common-gms\nC=common-gms\n")
        self.write("env/docker-free/all.env", "B=docker-free-all\nC=docker-free-all\n")
        self.write("env/docker-free/gms.env", "C=docker-free-gms\n")
        self.write("smoke-test/env/all.env", "D=smoke\n")
        env = self.resolve(job="smoke-test")
        self.assertEqual(env["A"], ("common-gms", "env/common/gms.env"))
        self.assertEqual(env["B"][0], "docker-free-all")
        self.assertEqual(env["C"][0], "docker-free-gms")
        self.assertEqual(env["D"], ("smoke", "smoke-test/env/all.env"))
        self.assertNotIn("D", self.resolve())

    def test_service_files_only_reach_their_services(self) -> None:
        self.write("env/docker-free/gms.env", "G=1\n")
        self.write("env/docker-free/system-update.env", "S=1\n")
        self.write("env/docker-free/worker.env", "I=1\n")
        self.assertEqual(set(self.resolve("gms")), {"G"})
        self.assertEqual(set(self.resolve("system-update")), {"G", "S"})
        self.assertEqual(set(self.resolve("restore-indices")), {"G"})
        self.assertEqual(set(self.resolve("worker")), {"I"})

    def test_local_and_ci_variants_are_exclusive(self) -> None:
        self.write("env/docker-free/gms.env", "X=base\n")
        self.write("env/docker-free/local/gms.env", "X=local\n")
        self.write("env/docker-free/ci/gms.env", "X=ci\n")
        self.assertEqual(self.resolve(mode="local")["X"][0], "local")
        self.assertEqual(self.resolve(mode="ci")["X"][0], "ci")

    def test_adhoc_files_apply_last_and_respect_scope(self) -> None:
        self.write("env/docker-free/all.env", "X=layer\n")
        first = self.write("tmp/first.env", "X=first\n")
        second = self.write("tmp/second.env", "X=second\n")
        gms_only = self.write("tmp/gms.env", "Y=gms-only\n")
        adhoc = [EnvFile(first), EnvFile(second), EnvFile.parse(f"gms:{gms_only}")]
        self.assertEqual(self.resolve(adhoc=adhoc)["X"][0], "second")
        self.assertEqual(self.resolve("system-update", adhoc=adhoc)["Y"][0], "gms-only")
        self.assertNotIn("Y", self.resolve("runner", adhoc=adhoc))

    def test_missing_adhoc_file_fails(self) -> None:
        with self.assertRaises(EnvLayerError):
            self.resolve(adhoc=[EnvFile(self.root / "nope.env")])

    def test_placeholders_expand_from_context(self) -> None:
        self.write(
            "env/docker-free/gms.env", "URL=http://localhost:${GMS_LITE_PORT}\nP=a$$b\n"
        )
        env = self.resolve(context={"GMS_LITE_PORT": "8090"})
        self.assertEqual(env["URL"][0], "http://localhost:8090")
        self.assertEqual(env["P"][0], "a$b")

    def test_unknown_placeholder_or_bare_dollar_fails(self) -> None:
        for value in ("${GMS_LITE_NOPE}", "a$b", "${OTHER:-x}"):
            self.write("env/docker-free/gms.env", f"X={value}\n")
            with self.assertRaises(EnvLayerError, msg=value):
                self.resolve()

    def test_shell_overrides_only_opted_in_keys(self) -> None:
        self.write(
            "env/docker-free/gms.env",
            "OPT=${OPT:-port-${GMS_LITE_PORT}}\nPLAIN=file\n",
        )
        overrides = self.write("tmp/overrides.env", "LATE=${LATE:-default}\n")
        context = {"GMS_LITE_PORT": "1"}
        shell = {"OPT": "from-shell", "PLAIN": "from-shell", "LATE": "from-shell"}
        env = self.resolve(context=context, shell=shell, adhoc=[EnvFile(overrides)])
        self.assertEqual(
            env["OPT"], ("from-shell", "shell (opted in by env/docker-free/gms.env)")
        )
        self.assertEqual(env["PLAIN"][0], "file")
        self.assertEqual(env["LATE"][0], "from-shell")
        self.assertEqual(self.resolve(context=context)["OPT"][0], "port-1")

    def test_plain_adhoc_value_pins_an_opted_in_key(self) -> None:
        self.write("env/docker-free/gms.env", "OPT=${OPT:-default}\n")
        overrides = self.write("tmp/overrides.env", "OPT=pinned\n")
        env = self.resolve(shell={"OPT": "from-shell"}, adhoc=[EnvFile(overrides)])
        self.assertEqual(env["OPT"][0], "pinned")


class EnvFileParseTest(unittest.TestCase):
    def test_scope_prefix_is_optional(self) -> None:
        self.assertEqual(EnvFile.parse("gms:a.env"), EnvFile(Path("a.env"), "gms"))
        self.assertEqual(EnvFile.parse("a.env"), EnvFile(Path("a.env")))
        self.assertEqual(EnvFile.parse("weird:a.env"), EnvFile(Path("weird:a.env")))


if __name__ == "__main__":
    unittest.main()
