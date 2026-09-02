import pytest

from de_tech_radar import main


def test_main_prints_project_name(
    capsys: pytest.CaptureFixture[str],
) -> None:
    main()

    assert capsys.readouterr().out == "Hello from de-tech-radar!\n"
