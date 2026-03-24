# Customization

## Customizing Styling

Several elements of the diagram's style can be easily modified to align with your organization's naming conventions. The most common modifications are changes to the dbt model layer names, their abbreviations use as a model name prefix, and their node fill color in diagrams.

The following is the default list of model layers and their associated abbreviations. 
``` python
# The following is a list of recognized model layers and their abbreviated prefixes:
#   staging:      stg_
#   intermediate: int_
#   base:         bas_
#   fact:         fct_
#   dimension:    dim_
#   report:       rpt_
```

Both the model layer name prefix and the node background color that is assigned to that model layer is defined in the following dictionary `color_choices_pastel28`. Users can create their own pallet and update the dict `catagory_colors` accordingly.

``` python
default_node_color = "/pastel28/4"  # use this for an unknown/unspecified prefix: light plumb
category_colors = defaultdict(lambda: default_node_color)
color_choices_pastel28 = {
    "stg_": "/pastel28/6",  # yellow
    "int_": "/pastel28/2",  # orange yellow
    "bas_": "/pastel28/7",  # sandstone
    "fct_": "/pastel28/1",  # light green
    "dim_": "/pastel28/3",  # light blue grey
    "rpt_": "seashell",     # this color is off palette - keep reports mostly colorless
}

category_colors.update(color_choices_pastel28)  # the preferred color scheme
```



