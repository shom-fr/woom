{% extends "example.rst" %}



{% block tasks_cfg %}
{{ super() }}

Each subsection of the ``fill`` section  (e.g., ``model_config``, ``param_file``) defines which templates to fill:

- ``template``: The Jinja2 template file to use (located in the templates directory)
- ``destination``: Where to write the filled file (supports template variables)

The filled files will be generated in the :file:`output/` directory with
cycle-specific names like :file:`config_2020-01-01.nml`.
{% endblock %}

{% block running %}
Using the fill CLI command
---------------------------
You can also fill templates manually using the ``woom fill`` command:

.. code-block:: bash

    woom fill model.nml.j2 output/config.nml --task-name hydro_model --cycle 2020-01-01

This is useful for testing templates or generating one-off configuration files.

See also: :ref:`cli.woom.fill`.

{{ super() }}
{% endblock %}
