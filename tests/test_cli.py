import pytest
from pathlib import Path
from click.testing import CliRunner
from elote.cli import cli


class TestInitCommand:
    def test_init_creates_conf_directory(self, temp_dir, monkeypatch):
        """init command creates conf/ directory."""
        monkeypatch.chdir(temp_dir)
        runner = CliRunner()

        result = runner.invoke(cli, ["init"])

        assert result.exit_code == 0
        assert (temp_dir / "conf").is_dir()

    def test_init_creates_output_directory(self, temp_dir, monkeypatch):
        """init command creates output/ directory."""
        monkeypatch.chdir(temp_dir)
        runner = CliRunner()

        result = runner.invoke(cli, ["init"])

        assert result.exit_code == 0
        assert (temp_dir / "output").is_dir()

    def test_init_creates_datasets_csv(self, temp_dir, monkeypatch):
        """init command creates conf/datasets.csv."""
        monkeypatch.chdir(temp_dir)
        runner = CliRunner()

        result = runner.invoke(cli, ["init"])

        assert result.exit_code == 0
        datasets_csv = temp_dir / "conf" / "datasets.csv"
        assert datasets_csv.exists()
        content = datasets_csv.read_text()
        assert "year,start_date,end_date,field_reference_file,source_file" in content

    def test_init_creates_field_reference_json(self, temp_dir, monkeypatch):
        """init command creates conf/field_reference.json."""
        monkeypatch.chdir(temp_dir)
        runner = CliRunner()

        result = runner.invoke(cli, ["init"])

        assert result.exit_code == 0
        field_ref = temp_dir / "conf" / "field_reference.json"
        assert field_ref.exists()
        content = field_ref.read_text()
        assert "in_types" in content
        assert "renames" in content

    def test_init_creates_process_py(self, temp_dir, monkeypatch):
        """init command creates process.py."""
        monkeypatch.chdir(temp_dir)
        runner = CliRunner()

        result = runner.invoke(cli, ["init"])

        assert result.exit_code == 0
        process_py = temp_dir / "conf" / "process.py"
        # Actually it's in the root, not conf/
        process_py = temp_dir / "process.py"
        assert process_py.exists()
        content = process_py.read_text()
        assert "from elote import transform_dataset, load_dataset" in content

    def test_init_skips_existing_files(self, temp_dir, monkeypatch):
        """init command does not overwrite existing files."""
        monkeypatch.chdir(temp_dir)
        runner = CliRunner()

        # Create conf dir and datasets.csv with custom content
        (temp_dir / "conf").mkdir()
        datasets_csv = temp_dir / "conf" / "datasets.csv"
        datasets_csv.write_text("custom content")

        result = runner.invoke(cli, ["init"])

        assert result.exit_code == 0
        assert "Skipped" in result.output
        assert datasets_csv.read_text() == "custom content"

    def test_init_shows_next_steps(self, temp_dir, monkeypatch):
        """init command shows next steps."""
        monkeypatch.chdir(temp_dir)
        runner = CliRunner()

        result = runner.invoke(cli, ["init"])

        assert result.exit_code == 0
        assert "Next steps" in result.output
