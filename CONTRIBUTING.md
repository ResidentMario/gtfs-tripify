## Development

### Cloning

To work on `gtfs_tripify` locally, you will need to clone it.

```sh
git clone https://github.com/ResidentMario/gtfs-tripify.git
```

You can then set up your own branch version of the code, and work on your changes for a pull request from there.

```sh
cd gtfs_tripify
git checkout -B new-branch-name
```

### Environment

I strongly recommend creating a new virtual environment when working on `gtfs_tripify` (e.g. not using the base system Python). I recommend doing so with [`conda`](https://conda.io/) or [`pipenv`](https://github.com/pypa/pipenv).

You should then create an [editable install](https://pip.pypa.io/en/latest/reference/pip_install/#editable-installs) of `gtfs_tripify` suitable for tweaking and further development. Do this by running:

```sh
pip install -e gtfs_tripify .[develop]
```

Note that `gtfs_tripify` is currently Python 3.6+ only.

### Tests

`gtfs_tripify` is thoroughly tested. There are three sets of test files: `core_tests.py`, which contains tests for core data integration logic; `io_tests.py`, which contains tests for IO method (like `to_csv`); and `util_tests.py`, which contains tests for user-facing utility methods. These tests can be run with `pytest` with e.g.:

```sh
pytest core_tests.py
```

Any pull requests to this repo are expected to pass all tests, and to add tests for any new features or changes in behavior to the relevant test file(s).

## Documentation

`gtfs_tripify` documentation is generated via [`sphinx`](http://www.sphinx-doc.org/en/stable/index.html) and served using [GitHub Pages](https://pages.github.com/). You can access it [here](https://residentmario.github.io/gtfs-tripify/index.html).

The website is automatically updated whenever the `gh-pages` branch on GitHub is updated. `gh-pages` is an orphan branch containing only documentation. The root documentation files are kept in the `master` branch, then pushed to `gh-pages` by doing the following:

```sh
git checkout gh-pages
rm -rf *
git checkout master -- docs/
cd docs; make html; cd ..
mv ./docs/_build/html/* ./
rm -rf docs
```

The data analysis demo is unique in that it is notebook-based. To update this page, update and rerun the `data_analysis_demo.ipynb` file in the `notebooks` folder, then run `jupyter nbconvert --to rst data_analysis_demo.ipynb`, then move the resultant ``data_analysis_demo.rst`` file and ``data_analysis_demo_files`` folder to the ``docs`` folder, *before* running the update procedure above.

So to update the documentation, edit the `.rst` files in the `docs` folder, then run `make html` there from the command line (optionally also running `make clean` beforehand). Then follow the procedure linked above to make these changes live.
