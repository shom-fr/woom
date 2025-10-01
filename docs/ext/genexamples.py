"""Generate rst files for examples"""

import os

TEMPLATE_ENTRY = {
    "realistic": """
.. _examples.{section}.{path}:

.. include:: ../../examples/{section}/{path}/README.rst

Configuring
-------------

.. literalinclude:: ../../examples/{section}/{path}/workflow.cfg
    :language: ini
    :caption: :file:`workflow.cfg`

.. literalinclude:: ../../examples/{section}/{path}/tasks.cfg
    :language: ini
    :caption: :file:`tasks.cfg`

.. literalinclude:: ../../examples/{section}/{path}/hosts.cfg
    :language: ini
    :caption: :file:`hosts.cfg`

Running
-------

Overview
~~~~~~~~
Let's have an overview of stages before running the workflow.

.. command-output:: woom show overview
    :cwd: ../../examples/{section}/{path}

Dry run
~~~~~~~
Now let's run the workflow in test (dry) and debug modes.

.. command-output:: woom run --log-no-color --log-level debug --dry-run
    :cwd: ../../examples/{section}/{path}

"""
}

TEMPLATE_ENTRY["academic"] = (
    TEMPLATE_ENTRY["realistic"]
    + """
Normal run
~~~~~~~~~~
And finally in run it.

.. command-output:: woom run --log-no-color
    :cwd: ../../examples/{section}/{path}

Check status
~~~~~~~~~~~~
Check what is running or finished.

.. command-output:: woom show status
    :cwd: ../../examples/{section}/{path}

Show run directories
~~~~~~~~~~~~~~~~~~~~
Show where tasks were executed.

.. command-output:: woom show run_dirs
    :cwd: ../../examples/{section}/{path}


"""
)

TEMPLATE_INDEX = """
.. _examples:

Examples of configuration
=========================

Academic examples
-----------------

.. toctree::
    :maxdepth: 1

{toc_entries[academic]}


Realistic examples
------------------

.. toctree::
    :maxdepth: 1

{toc_entries[realistic]}


"""


def genexamples(app):
    srcdir = app.env.srcdir

    gendir = os.path.join(srcdir, "examples")
    if not os.path.exists(gendir):
        os.makedirs(gendir)

    exdir = os.path.join(srcdir, "..", "examples")
    entries = {}
    toc_entries = {}
    for section in "academic", "realistic":
        secdir = os.path.join(exdir, section)
        toc_entries[section] = ""
        if not os.path.exists(secdir):
            continue
        for path in os.listdir(secdir):
            readme = os.path.join(secdir, path, "README.rst")
            if os.path.exists(readme):
                entries[path] = os.path.join(gendir, f"{path}.rst")
                toc_entries[section] += "    " + path + "\n"

                content = TEMPLATE_ENTRY[section].format(**locals())
                prolog_rst = os.path.join(secdir, path, "prolog.rst")
                if os.path.exists(prolog_rst):
                    with open(prolog_rst) as f:
                        content += "\n" + f.read().format(**locals())
                with open(entries[path], "w") as fe:
                    fe.write(content)
    with open(os.path.join(gendir, "index.rst"), "w") as fi:
        fi.write(TEMPLATE_INDEX.format(**locals()))


def setup(app):
    app.connect("builder-inited", genexamples)

    return {"version": "0.1"}
