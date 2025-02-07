import os

TEMPLATE_ENTRY = {}
TEMPLATE_ENTRY[
    "realistic"
] = """
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

"""

TEMPLATE_ENTRY["academic"] = (
    TEMPLATE_ENTRY["realistic"]
    + """
Running
-------

Let's have an overview of stages before running the workflow.

.. command-output:: woom overview
    :cwd: ../../examples/{section}/{path}

Now let's run the workflow.

.. command-output:: woom run --log-no-color
    :cwd: ../../examples/{section}/{path}


Check what is running or finished.

.. command-output:: woom status
    :cwd: ../../examples/{section}/{path}

"""
)

TEMPLATE_INDEX = """
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
                with open(entries[path], "w") as fe:
                    fe.write(TEMPLATE_ENTRY[section].format(**locals()))
    with open(os.path.join(gendir, "index.rst"), "w") as fi:
        fi.write(TEMPLATE_INDEX.format(**locals()))


def setup(app):

    app.connect("builder-inited", genexamples)

    return {"version": "0.1"}
