# 🔬 Developer Guide
Thank you for considering to contribute to the project! This guide will help you
to get started with the development of the project. If you have any questions,
please feel free to ask them in the issue tracker.

## Formal Requirements
The project uses documentation format [Napoleon](
https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
) with a [Sphinx](https://www.sphinx-doc.org/en/master/) autodoc GitHub
Actions workflow. If you add new code, please make sure that it is documented
accordingly and in a consistent manner with the existing code base. To check,
you can run the documentation build locally by running `make html` in the `docs`
directory.

We use [Poetry](https://python-poetry.org) for dependency management. Please
make sure that you have set up the environment correctly before starting
development. If you want to fix dependency issues, please do so in the Poetry
framework. If Poetry does not work for you for some reason, please let us know.

## Testing
The project uses [pytest](https://docs.pytest.org/en/stable/) for testing. To
run the tests, please run `pytest` in the root directory of the project. We are
developing BioCypher using test-driven development. Please make sure that you
add tests for your code before submitting a pull request.

The existing tests can also help you to understand how the code works. If you
have any questions, please feel free to ask them in the issue tracker.

**Before submitting a pull request, please make sure that all tests pass and
that the documentation builds correctly.**

## Small Contributions
If you want to contribute a small change (e.g. a bugfix), you can probably
immediately go ahead and create a pull request. For more substantial changes or
additions, please read on.

## Larger Contributions
If you want to contribute a larger change, please create an issue first. This
will allow us to discuss the change and make sure that it fits into the project.
It can happen that development for a feature is already in progress, so it is
important to check first to avoid duplicate work. If you have any questions,
feel free to approach us in any way you like.
