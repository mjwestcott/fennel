from fennel.worker import autodiscover


def test_autodiscover(mocker, tmp_path, app):
    mocked_importlib = mocker.patch("fennel.worker.discovery.importlib")
    app.settings.autodiscover = "**/tasks.py"
    tmp_path.touch("tasks.py")
    tasks = tmp_path / "tasks.py"
    tasks.write_text("def foo(): pass")

    autodiscover(app, basedir=tmp_path)
    module = ".".join(tasks.parts)[:-3]
    assert mocked_importlib.import_module.mock_calls == [mocker.call(module)]


def test_autodiscover_nested(mocker, tmp_path, app):
    mocked_importlib = mocker.patch("fennel.worker.discovery.importlib")
    app.settings.autodiscover = "**/tasks.py"
    testapp = tmp_path / "foo" / "bar"
    testapp.mkdir(parents=True)
    testapp.touch("tasks.py")
    tasks = testapp / "tasks.py"
    tasks.write_text("def foo(): pass")

    autodiscover(app, basedir=tmp_path)
    module = ".".join(tasks.parts)[:-3]
    assert mocked_importlib.import_module.mock_calls == [mocker.call(module)]


def test_autodiscover_disabled(mocker, tmp_path, app):
    mocked_importlib = mocker.patch("fennel.worker.discovery.importlib")
    app.settings.autodiscover = ""
    tmp_path.touch("tasks.py")
    tasks = tmp_path / "tasks.py"
    tasks.write_text("def foo(): pass")

    autodiscover(app, basedir=tmp_path)
    assert not mocked_importlib.import_module.called


def test_autodiscover_no_tasks(mocker, tmp_path, app):
    mocked_importlib = mocker.patch("fennel.worker.discovery.importlib")
    app.settings.autodiscover = "**/tasks.py"

    autodiscover(app, basedir=tmp_path)
    assert not mocked_importlib.import_module.called
