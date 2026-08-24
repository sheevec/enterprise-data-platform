"""CI/CD workflow contract: the YAML must keep its required jobs and guards."""

import pathlib

import yaml

WORKFLOWS = pathlib.Path(__file__).resolve().parents[2] / ".github" / "workflows"


class TestCiContract:
    def test_ci_yml_valid_and_has_core_jobs(self):
        doc = yaml.safe_load((WORKFLOWS / "ci.yml").read_text())
        jobs = set(doc["jobs"])
        assert {"lint", "test", "terraform"} <= jobs

    def test_test_job_pins_java_17(self):
        doc = yaml.safe_load((WORKFLOWS / "ci.yml").read_text())
        steps = doc["jobs"]["test"]["steps"]
        java_steps = [
            s for s in steps if isinstance(s, dict) and "setup-java" in str(s.get("uses", ""))
        ]
        assert java_steps, "test job must install a JDK for Spark"
        with_version = any(s.get("with", {}).get("java-version") == "17" for s in java_steps)
        assert with_version

    def test_cd_is_manual_and_environment_gated(self):
        doc = yaml.safe_load((WORKFLOWS / "cd.yml").read_text())
        assert doc[True] is not None or "workflow_dispatch" in str(doc.get("on"))
        deploy = doc["jobs"]["deploy"]
        assert deploy.get("environment"), "deploy must target a protected environment"

    def test_cd_terraform_apply_requires_explicit_flag(self):
        raw = (WORKFLOWS / "cd.yml").read_text()
        assert "if: ${{ inputs.apply_terraform }}" in raw
