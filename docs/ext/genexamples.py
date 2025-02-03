import os


TEMPLATE_ENTRY = """
.. include:: ../../examples/{path}/README.rst

.. literalinclude:: ../../examples/{path}/workflow.cfg
    :language: ini
    :caption: :file:`workflow.cfg`
    
.. literalinclude:: ../../examples/{path}/tasks.cfg
    :language: ini
    :caption: :file:`tasks.cfg`
    
.. literalinclude:: ../../examples/{path}/hosts.cfg
    :language: ini
    :caption: :file:`hosts.cfg`
    
"""

TEMPLATE_INDEX = """
Examples of configuration
=========================

.. toctree::
    
"""


def genexamples(app):

    srcdir = app.env.srcdir

    gendir = os.path.join(srcdir, "examples")
    if not os.path.exists(gendir):
        os.makedirs(gendir)

    exdir = os.path.join(srcdir, "..", "examples")
    entries = {}
    with open(os.path.join(gendir, "index.rst"), "w") as fi:
        fi.write(TEMPLATE_INDEX)

        for path in os.listdir(exdir):

            readme = os.path.join(exdir, path, "README.rst")
            if os.path.exists(readme):
                entries[path] = os.path.join(gendir, f"{path}.rst")
                with open(entries[path], "w") as fe:
                    fe.write(TEMPLATE_ENTRY.format(**locals()))

                fi.write(f"    {path}\n")


def setup(app):

    app.connect("builder-inited", genexamples)

    return {"version": "0.1"}
