.. _contributing:


Contributing guide
##################

Thank you for your interest in contributing to **woom**!

Reporting Issues
================

**Bug report:**

- Check existing issues first
- Include: OS, Python version, error message
- Provide a minimal reproducible example

**Feature request:**

- Describe the functionality and use case
- Show how you would use it

Contributing Code
==================

Quick start
-----------

1. Fork the repository
2. Clone and setup::

    git clone https://github.com/YOUR_USERNAME/woom.git
    cd woom
    python -m venv venv
    source venv/bin/activate
    pip install -e ".[dev,test]"

3. Create a branch::

    git checkout -b feature/my-feature  # or fix/my-fix

4. Make your changes and commit::

    git commit -m "Add feature: clear description"

Testing
-------

All contributions must include tests in :file:`tests/` directory.

Example of testing function :func:`woom.util.my_func` in file :file:`tests/test_util.py`::

    import pytest
    from woom import util

    def test_my_func():
        """Test :func:`woom.util.my_func`"""
        result = myfunc(10)
        aassert result == 11

Run tests::

    pytest                          # All tests
    pytest tests/test_conf.py       # Specific file

Usage Examples
--------------

Add examples, either in ``examples/academic/`` or ``examples/realistic/``, with the following files:

- ``README.md`` - what it demonstrates and how to run it
- Configuration files (``workflow.cfg``, ``tasks.cfg``, etc.)
- Required files, if any, in ``bin/``, ``ext/``, etc.

Example structure::

    examples/
    └── academic/
        └── my_example/
            ├── README.md
            ├── workflow.cfg
            ├──  tasks.cfg
            ├── bin/...
            ├── ext/...
            └──...

Pull Requests
=============

1. Push your branch::

    git push origin feature/my-feature

2. Create a PR on GitHub with:

   - Clear title and description
   - What changed and why
   - How to test
   - Reference related issues (e.g., "Closes #123")

3. Ensure:

   - 🗹 Tests pass
   - 🗹 Code follows PEP 8
   - 🗹 Documentation updated
   - 🗹 Commit messages are clear

Code Style
==========

- Follow PEP 8
- 4 spaces indentation
- Max line length: 110
- Use docstrings (numpy style)


Questions?
==========

Open an issue with the "question" label.

Thank you for contributing!
