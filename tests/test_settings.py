import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config import settings


class SettingsTests(unittest.TestCase):
    def test_resolve_credentials_path_uses_env_value(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            creds_path = Path(tmp_dir) / "creds.json"
            creds_path.write_text("{}", encoding="utf-8")

            previous = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds_path)
            try:
                resolved = settings.resolve_credentials_path()
                self.assertEqual(resolved, creds_path.resolve())
            finally:
                if previous is None:
                    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
                else:
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = previous


if __name__ == "__main__":
    unittest.main()
