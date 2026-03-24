#!/usr/bin/env python3
"""
dbt_dotdiag - Generate custom, publication-quality dbt lineage diagrams.

This file contains a CLI application that generated dbt model lineage graphs using the
model meta data contained the json file manifest.json that is created as a byproduct of
the dbt documentation build process (the command "dbt docs run"). Graphs can be generated
for various output formats: SVG, PNG, JPEG, and PDF, as well as a DOT script file.

Alpha Version: 0.1.0
"""
import sys
from pathlib import Path
from dataclasses import dataclass
from collections import defaultdict
import json
from pygraphviz import AGraph
import argparse
import textwrap
import re


# The maximum depth of child or parent lineage as guardrail
GENERATION_COUNT_MAX = 10

# -----------------------------------------------------------------------------------------------
# Model node background color:
#
# Model names may have a category prefix. The dict category_colors assigns a different color
# from the brewer pallet to the standard model categories. Any model name that does not have
# an expected prefix will have it's node fill color set to the default color.
#
# The following is a list of recognized categories and their abbreviated prefixes:
#   staging:      stg_
#   intermediate: int_
#   base:         bas_
#   fact:         fct_
#   dimension:    dim_
#   report:       rpt_
#
# From graphviz document on color schemes: https://graphviz.org/docs/attrs/colorscheme/
# ... For example, if colorscheme=oranges9 (from Brewer color schemes), then color=7 is
#     interpreted as color="/oranges9/7", the 7th color in the oranges9 colorscheme.
# -----------------------------------------------------------------------------------------------

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
color_choices_set311 = {
    "stg_": "/set311/2",  # pale yellow
    "int_": "/set311/3",  # purple
    "bas_": "/set311/6",  # brown orange
    "fct_": "/set311/1",  # green blue
    "dim_": "/set311/5",  # baby blue
    "rpt_": "/set311/9",  # light grey
}
category_colors.update(color_choices_pastel28)  # the preferred color scheme


@dataclass
class Model_Rec:
    """
    A lineage record scraped from the project manifest json file. Contains the name of
    the model/table and a list of parent models that are used (in SQL "FROM .."

    Attributes:
    - model_name: A string holding the name of the table or view (e.g., fct_user_sessions).
    - materialized: A string describing how the model is saved in the database
      (e.g., 'table', 'incremental', or 'view').
    - parent_models: A list of strings, where each string is the name of a model that
      this model directly depends on.
    """
    model_name: str
    materialized: str
    parent_models: list

class ManifestModels( ):
    """
    Contains all the methods required to extract the lineage records from the Manifest file,
    stitch together connecting relationships, and generate the output diagrams and dot script.
    """
    def __init__(self, manifest_path):
        self.manifest_path = manifest_path
        self.all_recs = []        # the parent lineage of all models in the Manifest's project.
        self.all_recs_dict = {}   # as a convenience, a dictionary of all records with
                                  # record model name as key
        self.graph_obj = None     # the graph object reference throughout the workflow

    def __build_records_dict__(self):
        """
        create a dict by model name for convenience
        """
        for rec in self.all_recs:
            self.all_recs_dict[rec.model_name] = rec

    def extract_all_records(self):
        """
        Scrapes the model lineage records from the manifest file
        """
        f = open(self.manifest_path)
        data = json.load(f)
        all_nodes = data["nodes"]

        for key, value in all_nodes.items():
            if value["resource_type"] == "model":
                # key has the format: model.<dbt project name>.<model name>
                # we only care about the model name
                model_name = key.split(".")[-1]
                materialized = value["unrendered_config"]["materialized"]
                parent_models = value["depends_on"]["nodes"]
                parent_names = [full_name.split(".")[-1] for full_name in parent_models]
                self.all_recs.append(Model_Rec(model_name=model_name,
                                               materialized=materialized, parent_models=parent_names))

        self.__build_records_dict__()

    def get_complete_model_list(self):
        """
        Gets the model name from the scraped node records
        """
        return list(self.all_recs_dict.keys())

    def models_are_valid(self, target_models):
        """
        Verifies that the named models in the list of targeted / featured models
        chosen by the user are found in the project.
        """
        for model_name in target_models:
            if self.all_recs_dict.get(model_name) is None:
                print("Error: model name not found in manifest: ", model_name)
                return False
        # validation passed: either the list was empty oo all target models were found
        return True


    def extract_parent_models(self, target_models, max_generations=2):
        """
        Given a set of target models, finds all direct ancestors (parents)
        in the usage relationships to a depth of max_generations.
        """
        models_to_graph = []
        models_to_graph += target_models  # by definition, these as a minimum
        available_models = self.get_complete_model_list()

        generation_cnt = 1
        while generation_cnt <= max_generations:
            # this contains the new used tables found in the current pass
            prev_generations_models = [ ]
            for model in models_to_graph:
                if model in available_models:
                    # The first pass of this for loop adds the generation's parent tables
                    # to the models_to_graph list. The next pass processes this expanded
                    # list of models. To avoid processing a model more than once and
                    # adding duplicates to the list, we pop these models from the
                    # available_models dict.
                    # -------------------------------------------------------------------
                    model_rec = self.all_recs_dict[model]
                    prev_generations_models += model_rec.parent_models
                    available_models.remove(model)

            # After each pass we add the found uses models to the cumulative
            # uses models to begin extracting models for the next depth.
            # ---
            models_to_graph += prev_generations_models
            generation_cnt += 1

        # completed the full search
        # now deduplicate the list
        clean_models_to_graph = list(set(models_to_graph))
        clean_models_to_graph.sort()
        return clean_models_to_graph

    def extract_children_models(self, target_models, max_generations=2):
        """
        Given a set of target models, finds all direct descendants (children)
        in the usage relationships to a depth of max_generations.
        """
        models_to_graph = []
        models_to_graph += target_models  # by definition, these as a minimum
        available_models = list(self.all_recs_dict.keys())

        generation_cnt = 1
        while generation_cnt <= max_generations:
            # this contains the new used tables found in the current pass
            next_generations_models = []
            # Search through the projects list of models and select those who have
            # parent models in the models_to_graph list. For the first pass, only the
            # core_models will be compared.
            for model in available_models.copy():  # needed because of remove
                # If any member of the models_to_graph match this model's parent list
                # then the model is a 'child' and we add this to the next generation model.
                #
                # The first pass of while loop adds the generations parent models
                # to the models_to_graph list. The next pass processes this expanded
                # list of models. To avoid processing a model more than once and
                # adding duplicates to the list, we pop these models from the
                # available_models dict.
                # -------------------------------------------------------------------
                model_rec = self.all_recs_dict[model]
                is_child = bool(set(model_rec.parent_models) & set(models_to_graph))
                if is_child:
                    next_generations_models.append(model)
                    available_models.remove(model)

            # After each pass we add the found uses models to the cumulative
            # uses models to begin extracting models for the next depth.
            # ---
            models_to_graph += next_generations_models
            generation_cnt += 1

        # completed the full search ...
        # now deduplicate the list ...
        clean_models_to_graph = list(set(models_to_graph))
        clean_models_to_graph.sort()
        return clean_models_to_graph

    def filter_models(self, initial_combined_models, regex_exclusion_str):
        """
        Given a list of model names, tests each name for a match with the regex, returning
        a new list with the matches excluded.
        """
        final_combined_models = []
        excluded_models = []
        rexp_comp = re.compile(regex_exclusion_str)

        for model in initial_combined_models:
            if rexp_comp.search(model) is None:
                final_combined_models.append(model)
            else:
                excluded_models.append(model)

        print("excluded models: \n", excluded_models)
        return final_combined_models


    def prepare_graph(self, featured_models, parent_depth, child_depth, model_list,
                      filter_pattern, num_filtered_models,
                      title,
                      show_details,
                      layout="dot"):
        """
        Creates and completes all aspects of the graph object ready for rendering.
        """

        def __node_fill_color__(model):
            """
            A convenience function to assign a background color to a table's node.
            """
            prefix = model[:4]
            return category_colors[prefix]

        def __node_style__(model):
            """
            Given the model name, determines what styling attributes to apply to the node.
            Note that we need to add "filled" to style in order for the fillcolor to be active;
            it appears that configuring style without it defaults to no fill.
            """
            # materialized = self.all_recs_dict[model].materialized
            # the model may not be in all_recs_dict
            default_style = 'solid'
            rec_found = self.all_recs_dict.get(model)
            if rec_found is not None:
                materialized = rec_found.materialized
                if materialized == "table":
                    style = 'solid,bold,filled'
                elif materialized == 'incremental':
                    style = 'dashed,bold,filled'
                elif materialized == 'view':
                    # Note: Graphviz provides to ways for changing the with of a node's border:
                    # 1] the penwidth node attribute (stated as preferred) or 2] as a valid
                    # argument in a node's style string, e.g. style="filled,blablah,setlinewidth(3)"
                    # We use setlinewith here to safe effort since we can simply piggy-back on style
                    style = 'setlinewidth(0),bold,filled'  # formerly dotted,rounded,filled
                else:
                    style = default_style
                return style
            else:
                return default_style

        if len(featured_models) > 4:
            # Too many featured models name to print in graph label, so print none of them.
            # Future: add code recognize when the full project is being graphed
            # todo  and add an appropriate comment.
            featured_models_text = "(not shown - more than four)"
        else:
            aligned_featured_models = ['\t' + model + r'\l' for model in featured_models]
            featured_models_text = "".join(aligned_featured_models)

        # Notes:
        #
        # This is the figure's label text, effectively the title and subtitle.
        # A bounding rectangle is created around this multi-line text description.
        # By default, each line is centred within the left and right boundaries.
        #
        # In order to achieve a left justification at the line level, lines of text must be
        # preceded by a \l rather than a \n, and setting the graph attribute nojustify to True.
        #
        # tips: graphviz documentation has sprinkled links to their online "playground" app that
        # allows you to test and modify short examples: https://graphviz.org/docs/attrs/nojustify/
        # also site https://renenyffenegger.ch/notes/tools/Graphviz/examples/index
        # -------

        # Initialize the title and config text to empty strings.
        # Note: There is only a single 'label' object available to add title type annotations
        # to the final diagram. Some level of styling can be added to the label through
        # html tags, but this is not as straight forward as you would expect due to
        # the interpretation and an limitations of Graphviz.
        # ---
        title_text_final = ''
        if title is not None:
            title_text_final = title +  r'\l \l'

        details_text_final = ''
        if show_details:
            details_text_final = \
            r"Target models: \l " + featured_models_text + r"\l" + \
            "Parent depth: " + str(parent_depth) + ", " + "Child depth:  " + str(child_depth) + r"\l" + \
            "Model filter regex: " + str(filter_pattern)  +  r"\l" +\
            "# models excluded: " + str(num_filtered_models) + r"\l" + \
            r"Border style: [ solid = table, dashed = incremental table, none = view ] \l"

        graph_attr = {
            'layout': layout,
            'label': title_text_final + details_text_final,
            'labelloc': 'b',
            'labeljust': 'l',  # left justify text, other options are "c" and "r"
            'nojustify': True,
            'directed': True,
            'rankdir': 'LR',
            'concentrate': 'true',
        }
        node_attr = {
            'style': 'filled',
            'shape': 'box',
            'arrowhead': 'normal',
        }
        edge_attr = {
            'arrowhead': 'normal',
            'arrowtail': 'dot',
        }

        # Important: some graph attributes appear to need to be set in the
        # __init__ as adding directed=True in a later
        # G.graph_attr.update(**graph_attr) did not product arrows!! (much time wasted)
        # while this did=: G = AGraph(directed=True)
        # ---
        G = AGraph(**graph_attr)
        G.node_attr.update(**node_attr)
        G.edge_attr.update(**edge_attr)

        # add the nodes
        # G.add_nodes_from(table_list)
        for model in model_list:
            G.add_node(model, fillcolor=__node_fill_color__(model), style=__node_style__(model))

        for model in model_list:
            if model in self.all_recs_dict.keys():
                model_rec = self.all_recs_dict[model]
                uses_list = model_rec.parent_models
                for use in uses_list:
                    if use in model_list:
                        # The graph is restricted to the list of table passed
                        # we want to avoid accidentally adding unwanted nodes
                        G.add_edge(use, model, arrowhead='empty')  # direction: used to user

        self.graph_obj = G

    def draw_graph(self, output_path, artifacts):
        """
        This method renders the dot instructions generated by method prepare_graph as graphic
        diagrams through a call to draw() and writes to file.
        NOTE: If user included 'dot' in the list of artifacts to render we call write()
        rather than draw(); this creates a dot file without any explicit node or edge
        positioning instructions (preferable) which would otherwise be included if draw()
        was used to create the dot file.
        """
        for artifact in artifacts:
            if artifact == 'dot':
                self.graph_obj.write(str(output_path) + '.' + artifact)
            else:
                self.graph_obj.draw(str(output_path) + '.' + artifact, format=artifact, prog="dot")
        if len(artifacts) == 0:
            print('Warning: no output format was selected')

# -----------------------------------------------------------------------------------------------
# The code in the remainder of this file relates to command line parsing, parameter validation,
# and the __main__ .
# -----------------------------------------------------------------------------------------------

def build_args( ):
    """
    This function generates the cmd line arguments for this app
    """

    description_text = \
    """
    Generates data lineage diagrams from dbt models defined in a dbt manifest json file.
    Provides options for restricting the scope of the diagram to a set of core 'target' models
    together with only 'parent' (uses) and 'child' (used by) models of that set.
    The usage depth can be expand to include multiple parent and child generations.
    """
    epilog_text = \
    """
    Treat this version of the application as a working prototype. 
    """
    # create the parser object
    parser = argparse.ArgumentParser(
        prog='dbt_diags',
        description=textwrap.dedent(description_text),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(epilog_text)
    )

    parser.add_argument('--manifest-path', type=str, required=True,
                        help='path to manifest file')
    parser.add_argument('--parent-depth', type=int, required=False,
                        help='optional depth of parent model ancestry, default is all')
    parser.add_argument('--child-depth', type=int, required=False,
                        help='optional depth of child model descendants, default is all')
    parser.add_argument('--target-models', type=str, required=False,
                        help='comma separated list of target models, default is all (the entire project)')
    parser.add_argument('--output-path', type=str, required=True,
                        help='path to output diagram file, excluding file name extension')
    parser.add_argument('--filter-models', type=str, required=False,
                        help='a search regular expression to exclude matching models')

    parser.add_argument('--title', type=str, required=False,
                        help='add an optional title to the diagram')
    parser.add_argument('--show-details', action='store_const', const=True,
                        help='add configuration details to diagram')

    parser.add_argument('--dot', action='store_const', const=True,
                        help='create separate DOT script text file named *.dot at output location')
    parser.add_argument('--svg', action='store_const', const=True,
                        help='create separate SVG format file named *.svg at output location')
    parser.add_argument('--png', action='store_const', const=True,
                        help='create separate PNG format file named *.png at output location')
    parser.add_argument('--pdf', action='store_const', const=True,
                        help='create separate PDF format file named *.pdf at output location')

    args = parser.parse_args()
    return args

def get_selected_artifacts(args):
    """
    Here we process all possible artifacts selected.
    """
    selected = [ ]
    if not args.dot is None:
        selected.append('dot')
    if not args.svg is None:
        selected.append('svg')
    if not args.png is None:
        selected.append('png')
    if not args.pdf is None:
        selected.append('pdf')
    return selected

def validate_lineage_depth(depth):
    """
    :param depth: expected to be a positive integer
    :return: either the original validated depth value or None in the case where original value
             is not a positive integer
    """
    if depth is None:
        # use the max
        return GENERATION_COUNT_MAX
    else:
        try:
            if isinstance(depth, int) and depth >= 0:
                return depth
            else:
                # not a positive integer
                return None
        except:
            # not a positive integer
            return None

def validate_manifest_path(uri):
    """
    Returns a valid URI for a manifest file. The DBT source file is named "manifest.json"
    but here we allow any name provide it has the json extension, to accommodate cloned and
    renamed manifest files for other purposes.

    :param uri:
        the manifest uri from the command line
    :return:
        validated uri; None if error encountered
    """
    path = Path(uri)

    if path.is_dir( ):
        print('Error: path is a directory, must include file name', uri)
        return None

    if not path.is_file( ):
        print('Error: path is not a file: ', uri)
        return None

    if path.suffix != '.json':  # Note: by definition suffix includes the "."
        print('Error: file is not json format:', uri, ', file extension: ', path.suffix)
        return None

    return path

def validate_output_path(uri):
    """
    Returns a valid uri for the output file. Verifies that the uri contains a valid
    directory and 'candidate' file name. We do not check whether the string representing
    the file name is legal w.r.t the OS - if it is not it will fail down the line
    at creation time.

    If the file name contains a suffix / extension it is stripped. The user will select
    one or more files to be generated via the pdf, csv, dot, and png command line keywords.

    :param uri:
        the output uri from the command line
    :return:
        validated uri minus any trailing extension; None if error encountered
    """
    path = Path(uri)
    stem, suffix = path.stem, path.suffix
    if path.is_dir( ):
        print('Error: output path is a directory, must include file name', uri)
        return None

    if not path.parents[0].is_dir( ):
        print('Error: output path does not contain a valid URI: ', uri)
        return None

    return path.parents[0].joinpath(stem)

def validate_filter_models(search_string):
    """
    Returns either the original passed parameter, which will have been validated
    as a legitimate regular expression, or None.
    """
    try:
        rexp_comp = re.compile(search_string)
    except:
        if not (search_string is None):
            print("Invalid regex pattern: ", search_string)
        return None
    return search_string

def validate_title(title):
    """
    This method added for consistency. Just return the string
    """
    if title is None:
        return None
    elif len(title) == 0:
        # Don't process an empty string
        return None
    else:
        return title

def validate_show_details(show_details):
    """
    This method added for consistency.
    """
    if show_details is None:
        return False
    else:
        return True

# ////////////////////////////////////// __main__ ////////////////////////////////////////////////

def main( ):

    args = build_args( )

    # We validate and scrape the manifest file for model lineage.
    #
    # But first, the following five command line parameters can be 'validated' without
    # opening the manifest file. We do this in advance to shake out four frequent
    # command line errors.
    # ---
    manifest_path = validate_manifest_path(args.manifest_path)
    output_path   = validate_output_path(args.output_path)
    parent_depth  = validate_lineage_depth(args.parent_depth)
    child_depth   = validate_lineage_depth(args.child_depth)
    filter_expression = validate_filter_models(args.filter_models)
    title = validate_title(args.title)
    show_details = validate_show_details(args.show_details)

    if manifest_path is None or \
        output_path is None or \
        parent_depth is None or \
        child_depth is None:
        exit(0)

    models_obj = ManifestModels(args.manifest_path)
    models_obj.extract_all_records()

    # validate the command line target model list if one is provided
    # which must be checked against the manifest's defined models
    if args.target_models is None:
        # pass the complete list of tables
        target_models = models_obj.get_complete_model_list()
    else:
        # parse the comma separated list
        target_models = args.target_models.split(',')
        if not models_obj.models_are_valid(target_models):
            exit(0)

    # At this point all command line arguments has been validated and are usable
    # The Manifest Model has been initialized, now proceed with generating the diagram artifacts.

    complete_parent_list = \
    models_obj.extract_parent_models(target_models=target_models,
                                     max_generations=parent_depth)
    complete_child_list = \
    models_obj.extract_children_models(target_models=target_models,
                                       max_generations=child_depth)

    combined_list = list(set(complete_parent_list + complete_child_list))

    num_filtered_models = 0
    if filter_expression is None:
        final_list = combined_list
    else:
        # we may have a valid expression that does not actually filter out models
        final_list = models_obj.filter_models(combined_list, filter_expression)
        num_filtered_models = len(combined_list) - len(final_list)


    models_obj.prepare_graph(featured_models=target_models,
                             parent_depth=parent_depth, child_depth=child_depth,
                             model_list=final_list,
                             filter_pattern=filter_expression,
                             num_filtered_models=num_filtered_models,
                             title=title,
                             show_details=show_details)

    selected_artifacts = get_selected_artifacts(args)

    models_obj.draw_graph(output_path=output_path, artifacts=selected_artifacts)

if __name__ == '__main__':
	try:
		main()
	except KeyboardInterrupt:
		sys.exit(1)
