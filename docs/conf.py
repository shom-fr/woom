# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

import woom

sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), "ext"))

# %% Project information
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "WOrflow manager for Ocean Models"
copyright = "2025, The Shom team"
author = "The Shom team"
version = woom.__version__
release = version

# %% General configuration
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    #    "sphinx.ext.linkcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "IPython.sphinxext.ipython_directive",
    "IPython.sphinxext.ipython_console_highlighting",
    "sphinxarg.ext",
    "genlogos",
    "genexamples",
    "sphinxcontrib.programoutput",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# A list of ignored prefixes for module index sorting.
modindex_common_prefix = ['woom.']

# %% Options for HTML output
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
html_theme_options = {
    "logo": {
        "image_light": "_static/woom-logo-light.png",
        "image_dark": "_static/woom-logo-dark.png",
    },
    "repository_url": "https://github.com/shom-fr/woom",
    "use_repository_button": True,
    "use_issues_button": True,
    "use_source_button": True,
    "path_to_docs": "docs/",
}

# %% Intersphinx
intersphinx_mapping = {
    "python": ("https://docs.python.org/fr/3/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "jinja2": ("https://jinja.palletsprojects.com/en/stable/", None),
    "configobj": ("https://configobj.readthedocs.io/en/latest/", None),
    "platformdirs": ("https://platformdirs.readthedocs.io/en/latest/", None),
    "psutil": ("https://psutil.readthedocs.io/en/latest/", None),
}

# %% Autosumarry
autosummary_generate = True
