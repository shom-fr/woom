.. _examples:

Examples of configuration
=========================

Academic examples
-----------------

.. toctree::
    :maxdepth: 1

{% for name in examples["academic"] %}
    academic/{{ name }}
{% endfor %}


{% if examples["realistic"] %}
Realistic examples
------------------

.. toctree::
    :maxdepth: 1

{% for name in examples["realistic"] %}
    realistic/{{ name }}
{% endfor %}
{% endif %}
