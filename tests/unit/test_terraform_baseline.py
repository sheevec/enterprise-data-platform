"""Static hygiene checks for the terraform/ baseline (no binary required).

These run in CI alongside `terraform validate` (which needs the binary) and
catch the boring-but-fatal stuff: missing module files, hardcoded secrets,
public buckets, force_destroy on data buckets.
"""

import pathlib
import re

TERRAFORM_DIR = pathlib.Path(__file__).resolve().parents[2] / "terraform"

REQUIRED_MODULE_FILES = {
    "iam": ["main.tf", "variables.tf"],
    "gcs": ["main.tf", "variables.tf"],
    "bigquery": ["main.tf", "variables.tf"],
}

SECRET_PATTERNS = [
    re.compile(r"(?i)(api_key|password|secret)\s*=\s*\"[^\"]{8,}\""),
    re.compile(r"\"AIza[0-9A-Za-z_-]{20,}\""),  # google api keys
    re.compile(r"-----BEGIN (RSA |EC )?PRIVATE KEY-----"),
]


class TestModuleStructure:
    def test_all_module_files_present(self):
        for module, files in REQUIRED_MODULE_FILES.items():
            for f in files:
                path = TERRAFORM_DIR / "modules" / module / f
                assert path.exists(), f"missing {path}"

    def test_root_wires_every_module(self):
        main = (TERRAFORM_DIR / "main.tf").read_text()
        for module in REQUIRED_MODULE_FILES:
            assert f'source = "./modules/{module}"' in main

    def test_environment_tfvars_exist(self):
        assert (TERRAFORM_DIR / "environments" / "staging" / "terraform.tfvars").exists()
        prod_example = TERRAFORM_DIR / "environments" / "prod" / "terraform.tfvars.example"
        assert prod_example.exists()


class TestSecurityBaseline:
    def test_no_hardcoded_secrets_anywhere(self):
        offenders = []
        for tf in TERRAFORM_DIR.rglob("*.tf"):
            text = tf.read_text()
            for pattern in SECRET_PATTERNS:
                if pattern.search(text):
                    offenders.append(str(tf))
        assert not offenders, f"possible hardcoded secrets in: {offenders}"

    def test_buckets_block_public_access_and_survive_destroys(self):
        gcs_main = (TERRAFORM_DIR / "modules" / "gcs" / "main.tf").read_text()
        assert 'public_access_prevention    = "enforced"' in gcs_main
        assert "uniform_bucket_level_access = true" in gcs_main
        assert "force_destroy = false" in gcs_main

    def test_bq_datasets_refuse_content_destruction(self):
        bq_main = (TERRAFORM_DIR / "modules" / "bigquery" / "main.tf").read_text()
        assert "delete_contents_on_destroy = false" in bq_main

    def test_lifecycle_tiering_configured(self):
        gcs_main = (TERRAFORM_DIR / "modules" / "gcs" / "main.tf").read_text()
        assert "NEARLINE" in gcs_main
        assert "COLDLINE" in gcs_main

    def test_iam_grants_are_resource_scoped(self):
        iam_main = (TERRAFORM_DIR / "modules" / "iam" / "main.tf").read_text()
        # bucket-level grants, not project-wide objectAdmin
        assert "google_storage_bucket_iam_member" in iam_main
        assert "roles/storage.admin" not in iam_main
