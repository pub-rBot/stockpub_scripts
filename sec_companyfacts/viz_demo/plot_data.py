"""
Transform data into format suitable for making a time-lapse gif

End Goal: Make a timelapse gif for most popular metrics for selected stocks
Starting Material: SEC company facts dataset, CIK to ticker index file

To realize the end goal, we need to transform the data to standardized plots across time
For this demo, we will use Altair as the plotting package

Note:
    Not an Altair expert, nor affiliated with Altair
    But highly recommend checking them out
        great documentation
        helpful developers
        portability

"""

"""
Package imports
"""

import os
import shutil
import pandas as pd
from pathlib import Path
from os.path import exists
import sys
from os.path import join as os_path_join
import altair as alt

print("plot_data.py has started")

"""
HOUSE KEEPING CODE BLOCKS
"""

"""
General options and vars used throughout the script
"""

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
date_format = "%Y-%m-%d"
"""
Relative Paths
"""

script_path = os.path.abspath(os.path.realpath(__file__))  # current script path as str
pathlib_path = Path(script_path)   # path lib object of current script's path, make it easier to transverse the path tree
script_p1_name = str(pathlib_path.parent.parent.resolve().name)
script_p3_path = str(pathlib_path.parent.parent.parent.parent.resolve())

data_root_folder = os_path_join(script_p3_path, "stockpub_data")
dataset_folder = os_path_join(data_root_folder, script_p1_name)
data_folder = os_path_join(dataset_folder, "data")
time_machine_folder = os_path_join(data_folder, "time_machine")
support_folder = os_path_join(data_folder, "support")

derived_data_root_folder = os_path_join(script_p3_path, "stockpub_data_derived")
script_parent_name = str(pathlib_path.parent.resolve().name)
viz_demo_folder = os_path_join(derived_data_root_folder, script_parent_name)
viz_data_folder = os_path_join(viz_demo_folder, "data")

# remake a viz_plots folder everytime the script is run
viz_plots_folder = os_path_join(viz_demo_folder, "plots")
if not exists(viz_plots_folder):
    os.makedirs(viz_plots_folder)
else:
    shutil.rmtree(viz_plots_folder)
    os.makedirs(viz_plots_folder)

# Get the data files
viz_files = os.listdir(viz_data_folder)
viz_files = [x for x in viz_files if x.endswith(".feather")]
viz_files.sort()
# Get the metric name label description file from support folder
metrics_nld_fileloc = os_path_join(support_folder, "metrics_nld.feather")
metrics_nld_df = pd.read_feather(metrics_nld_fileloc)
# drop rows that does not have a label or description
metrics_nld_df.dropna(subset=['label', 'description'], how='all', inplace=True)

viz_file_loop = 0
viz_files_len = len(viz_files)
for viz_file in viz_files:  # loop through data files and make plots
    viz_file_loop += 1
    viz_file_date = viz_file.replace(".feather", "")

    # print out a message to let us know the plotting progress
    sys.stdout.write("\r \r {0}".format("Date: %s,  %s / %s" % (viz_file_date, str(viz_file_loop), str(viz_files_len))))
    sys.stdout.flush()

    viz_data_file_loc = os_path_join(viz_data_folder, viz_file)
    viz_data_df = pd.read_feather(viz_data_file_loc)

    if viz_file_loop != viz_files_len:
        # if it is not the most current date we are plotting, we will use the following as tool tip inside the plots
        tool_tip = ['ticker', 'unit_name', 'metric_name', 'cik_str', 'val', 'xformed_val']
    else:

        # if it is the most current date we are plotting,
        # we will merge the viz_data_df with metrics_nld_df based on metric_name
        try:
            # merge two dataframe using metric as key by first renaming metric to metric_name in metrics_nld_df
            # we merge based on viz_data_df
            viz_data_df = viz_data_df.merge(metrics_nld_df.rename({'metric': 'metric_name'}, axis=1), on='metric_name', how='left')
        except Exception as e:
            viz_data_df['label'] = None
            viz_data_df['description'] = None
            print(e)

        # we will add label and description to the tool tip inside the plots
        tool_tip = ['ticker', 'unit_name', 'metric_name', 'cik_str', 'val', 'xformed_val', 'label', 'description']

    """
    Plotting Codes
    """

    # declare a base chart for layering
    scatter_base = alt.Chart().properties(
        width=405,
        height=405,
    )
    # make a filter based on point by metric_name column value
    metric_selector = alt.selection_point(fields=['metric_name'], resolve='global')

    # extend the base chart and mark the data point with a circle
    # on the x-axis, we will use tickers with N (nominal) data type and rotate it 45 degrees
    # on the y-axis, we will use the transformed value with Q (quantitative) data type
    # we color each data point based on its metric name
    # for size, since all of our values are USD, we will scale the data point size based on them using custom scale values of 100 to 1000
    # for opacity, we will default to 0.5 if it is a filled in value, 0.9 if the value is from an actual filed date

    scatter_metrics = scatter_base.mark_circle().encode(
        x='ticker:N',
        y='xformed_val:Q',
        color=alt.Color("metric_name:N", legend=None),
        size=alt.Size("val:Q", scale=alt.Scale(range=[100, 1000]), legend=None),
        opacity=alt.condition(alt.expr.datum['actual'] == 0, alt.value(0.5), alt.value(0.9)),
    )

    # we add a horizontal line at y = 0 using mark_rule
    y_rule = alt.Chart(pd.DataFrame({
        'rule': [0],
    })).mark_rule(color="#71717a").encode(
        y='rule:Q',
    )

    # layer both chart on top of each other
    # declare the data source to be used
    # add metric_selector to the parameters, so it can be used in this chart and any chart that has this selector
    # override the defaults by declaring axis titles and other axis related parameters
    # add a tool tip to the chart so when we hover over it, it displays certain values
    # transforms the chart based on the selector if used
    scatter_layered = alt.layer(scatter_metrics, y_rule, data=viz_data_df).add_params(
        metric_selector,
    ).encode(
        x=alt.X(
            title=None,
            axis=alt.Axis(labels=False, grid=False)
        ),
        y=alt.Y(
            title='Metrics Values (Normalized)',
            scale=alt.Scale(domain=[-1.1, 1.1]),
            axis=alt.Axis(grid=False, ),
            stack=False
        ),
        tooltip=tool_tip,
    ).transform_filter(
        metric_selector
    )

    # make a chart base for the second chart, a histogram chart
    hist_base = alt.Chart(data=viz_data_df).properties(
        width=405,
        height=205,
    )

    # extend hist_base using mark_bar to make it a histogram chart
    # add the same metric_selector to the histogram chart so the scatter and histogram charts are linked
    # encode relevant displaying properties
    # change opacity based on metric_selector with selected having opacity = 1, otherwise = 0.5
    # use title parameter to make a footer for the chart
    hist_metrics = hist_base.mark_bar().add_params(
        metric_selector,
    ).encode(
        x=alt.X('ticker:N', title='Tickers', axis=alt.Axis(grid=False, labelAngle=0)),
        y=alt.Y(
            'val:Q',
            title='Metrics Values (Billion USD)',
            axis=alt.Axis(grid=False, labels=False),
        ),
        color=alt.Color("metric_name:N", legend=None),
        opacity=alt.condition(metric_selector, alt.value(1), alt.value(0.5)),
        tooltip=tool_tip,
    ).properties(
        title=alt.TitleParams(
            ['Click to filter', 'Hover for more info'],
            baseline='bottom',
            orient='bottom',
            anchor='end',
            fontWeight=200,
            fontSize=10,
            dy=0,
            dx=-5,
            zindex=99,
        )
    )

    # use vconcat to group the 2 charts on top of each other
    # declare a title for the grouped chart
    chart = alt.vconcat(scatter_layered, hist_metrics).properties(
        title=alt.TitleParams(text='Metrics Time-lapse', fontWeight="bold", fontSize=16, subtitle=["Date: " + viz_file_date], subtitleColor="teal", subtitleFontSize=14, subtitleFontWeight=500, align='center', anchor='middle')
    )

    # save the chart as image file to disk for making gif
    chart_file_loc = os_path_join(viz_plots_folder, (viz_file_date + ".png"))
    chart.save(chart_file_loc)

    if viz_file_loop == viz_files_len:
        # if it is the last loop, save the most current chart as a html file as well
        outputs_folder = os_path_join(viz_demo_folder, "outputs")
        if not exists(outputs_folder):
            os.makedirs(outputs_folder)
        chart_html_file_loc = os_path_join(outputs_folder, (viz_file_date + ".html"))
        chart.save(chart_html_file_loc)
        # print out interactive chart location to terminal
        print(f"\n Interactive chart saved to:\n {chart_html_file_loc}")

print("plot_data.py has finished")
up_next = """
UP NEXT:
see
make_gif.py
"""
print(up_next)
