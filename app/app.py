import os
import sys
from datetime import timedelta
import pandas as pd
import dash
from dash import dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import plotly.express as px

# Add the project root to PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pipeline.src.utils import (
    load_yaml, 
    load_parquet_from_gcs
    )
from pipeline.src.pipelines.clean import CleanData, load_clean_data
from pipeline.src.transform.results_transformer import wide_to_long_results
from pipeline.src.transform.utils import (
    add_all_cumsum_columns, 
    subset_most_recent_fight)
from utils.calcs import (
    compute_fighter_avg_interval, 
    compute_weight_class_distribution,
    compute_additional_stats,
    )
from utils.viz import (
    build_histogram, 
    build_fight_outcome_chart)

# Load the configuation data
config = load_yaml(os.path.join('pipeline', 'config', 'config.yaml'))

# Load the full clean dataset
clean_data: CleanData = load_clean_data(config=config)

# --------------- Data Loading ---------------
def read_data(clean_data: CleanData) -> pd.DataFrame:
    """
    Loads and preprocess the dataset for the app.
        1. Loads the cleaned results, fighter, and events dataframes.
        2. Transforms the results into wide format.
        3. Merges the other dataframes.
        4. Sorts by date, fight, and fighter.
    """

    fighter_opp: pd.DataFrame = (
        clean_data.fighters
        .copy()
        .rename(columns={"fighter_url": "opp_url", "full_name": "opp_full_name"})
        )[['fighter_url', 'full_name']]
   
    df: pd.DataFrame = (
        clean_data.results
        .pipe(wide_to_long_results)
        .merge(clean_data.fighters, on='fighter_url')
        .merge(fighter_opp, on='opp_url')
        .merge(clean_data.events, on='event_url')
        .sort_values(by=['date', 'fight_url', 'fighter_url'])
        .reset_index(drop=True)
    )
    return df

# Build full dataframe and computed stats
df = (
    read_data(config)
    .assign(result_method=lambda x: x['result'].str.lower() + '_' + x['method_type'].str.lower())
    .pipe(
        add_all_cumsum_columns,
        dummy_cols=['result', 'result_method', 'weight_class'],
        numerical_cols=['title_fight', 'perf_bonus', 'fight_of_the_night', 'fight_duration_seconds'],
        group_col='fighter_url',
        row_count_col='total_fights'
    )
)
df_current = df.pipe(
    subset_most_recent_fight,
    fighter_col='fighter_url',
    date_col='date'
)

overall_avg_intervals = compute_fighter_avg_interval(df)
# Also compute overall total fight time (in minutes) from seconds
overall_total_fight_times = df['total_fight_duration_seconds'] / 60

# --------------- Reusable Histogram Function ---------------


# Build histogram figures
hist_avg_interval_fig = build_histogram(
    overall_avg_intervals, 
    label='Avg Interval (days)', 
    title='Distribution of Avg Time Between Fights'
)
hist_total_fight_time_fig = build_histogram(
    overall_total_fight_times, 
    label='Total Fight Time (minutes)', 
    title='Distribution of Total Fight Time (minutes)'
)

# --------------- Dash App Setup ---------------
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "UFC Fighter Dashboard"

app.layout = dbc.Container([
    html.H1("UFC Fighter Dashboard", className="my-4 text-center"),
    dbc.Row([
        dbc.Col(
            dcc.Dropdown(
                id='fighter-dropdown',
                options=[
                    {'label': row['full_name'], 'value': row['fighter_url']}
                    for _, row in df_current.sort_values('full_name').iterrows()
                ],
                placeholder="Select a fighter",
                clearable=True,
                style={'fontSize': '16px'}
            ),
            width={"size": 6, "offset": 3}
        )
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(id='fighter-profile', width=12)
    ])
], fluid=True)


# --------------- Helper Functions for UI Components ---------------
def build_fighter_info_card(fighter, stats):
    fight_date_disp = fighter['date'].strftime('%B %d, %Y') if hasattr(fighter['date'], 'strftime') else fighter['date']
    ufc_record = f"{fighter.get('total_win', 0)}-{fighter.get('total_loss', 0)}-{fighter.get('total_draw', 0)}"
    total_fight_time_str = str(timedelta(seconds=int(fighter.get('total_fight_duration_seconds', 0))))
    # Convert fighter's total fight time to minutes for highlighting
    fighter_total_minutes = fighter.get('total_fight_duration_seconds', 0) / 60

    # Build the histogram for average interval with fighter's value highlighted
    fighter_avg_hist = build_histogram(
        overall_avg_intervals, 
        label='Avg Interval (days)', 
        title='Distribution of Avg Time Between Fights',
        highlight_value=stats['avg_interval_days'] if isinstance(stats['avg_interval_days'], (int, float)) else None
    )
    # Build the histogram for total fight time in minutes with fighter's value highlighted
    fighter_total_time_hist = build_histogram(
        overall_total_fight_times, 
        label='Total Fight Time (minutes)', 
        title='Distribution of Total Fight Time (minutes)',
        highlight_value=fighter_total_minutes
    )
    
    return dbc.Card(
        [
            dbc.CardHeader(html.H3(fighter['full_name'], className="text-center")),
            dbc.CardBody([
                # Personal Information
                html.H5("Personal Information", className="mb-2"),
                html.P(f"Nickname: {fighter['nickname']}" if fighter['nickname'] else "Nickname: N/A"),
                html.P(f"Birth Date: {fighter['date_of_birth'].strftime('%B %d, %Y') if hasattr(fighter['date_of_birth'], 'strftime') else fighter['date_of_birth']}"),
                html.P(f"Age: {stats['age']} years"),
                html.P(f"Overall Record: {fighter['record']}"),
                html.P(f"UFC Record: {ufc_record}"),
                html.Hr(),
                # Physical Attributes
                html.H5("Physical Attributes", className="mb-2"),
                html.P(f"Height: {fighter['height']} ({round(fighter['height_cm'], 2)} cm)"),
                html.P(f"Reach: {fighter['reach']} ({round(fighter['reach_cm'], 2)} cm)"),
                html.P(f"Stance: {fighter['stance']}"),
                html.Hr(),
                # Career Metrics
                html.H5("Career Metrics", className="mb-2"),
                html.P(f"Time Since Last Fight: {stats['time_since_last_days']} days"),
                html.P([
                    "Average Time Between Fights: ",
                    html.Span(f"{stats['avg_interval_days']} days", id="avg-interval-target")
                ]),
                dbc.Popover(
                    [
                        dbc.PopoverHeader("Avg Time Distribution"),
                        dbc.PopoverBody(
                            dcc.Graph(
                                id="avg-histogram-graph",
                                figure=fighter_avg_hist,
                                config={"displayModeBar": False}
                            )
                        ),
                    ],
                    id="avg-interval-popover",
                    target="avg-interval-target",
                    trigger="hover"
                ),
                html.P([
                    "Total Fight Time: ",
                    html.Span(total_fight_time_str, id="total-fight-time-target")
                ]),
                dbc.Popover(
                    [
                        dbc.PopoverHeader("Total Fight Time Distribution (minutes)"),
                        dbc.PopoverBody(
                            dcc.Graph(
                                id="total-time-histogram",
                                figure=fighter_total_time_hist,
                                config={"displayModeBar": False}
                            )
                        ),
                    ],
                    id="total-fight-time-popover",
                    target="total-fight-time-target",
                    trigger="hover"
                ),
                html.Hr(),
                # Recent Fight Details
                html.H5("Recent Fight Details", className="mb-2"),
                html.P(f"Outcome: {fighter['result']}"),
                html.P(f"Event: {fighter['event']}"),
                html.P(f"Fight Date: {fight_date_disp}")
            ])
        ],
        className="mb-4 shadow"
    )

def build_stats_card(fighter, weight_distribution):
    return dbc.Card(
        [
            dbc.CardHeader(html.H4("Cumulative Statistics", className="text-center")),
            dbc.CardBody([
                dcc.Graph(
                    id="stats-graph",
                    figure=build_fight_outcome_chart(fighter)
                ),
                html.Hr(),
                html.P("Weight Class Distribution: " + weight_distribution, style={'fontWeight': 'bold'}),
                html.Hr(),
                dbc.Row([
                    dbc.Col(html.Div([
                        html.H6("Title Fights:"),
                        html.P(fighter.get('total_title_fight', 0))
                    ]), md=4),
                    dbc.Col(html.Div([
                        html.H6("Performance Bonuses:"),
                        html.P(fighter.get('total_perf_bonus', 0))
                    ]), md=4),
                    dbc.Col(html.Div([
                        html.H6("Fight of the Night:"),
                        html.P(fighter.get('total_fight_of_the_night', 0))
                    ]), md=4)
                ], className="mb-3")
            ])
        ],
        className="mb-4 shadow"
    )

def build_fight_history_table(fights_df):
    fights = fights_df.copy()
    fights['fight_duration'] = fights['fight_duration_seconds'].apply(lambda s: str(timedelta(seconds=int(s))))
    fights['date'] = fights['date'].apply(lambda d: d.strftime('%B %d, %Y') if hasattr(d, 'strftime') else d)
    table_columns = ["date", "event", "weight_class", "opp_full_name", "method", "round", "time", "result",
                     "fight_duration", "title_fight", "perf_bonus", "fight_of_the_night"]
    table_data = fights[table_columns].to_dict('records')
    
    return dbc.Card(
        [
            dbc.CardHeader(html.H4("Fight History", className="text-center")),
            dbc.CardBody([
                dash_table.DataTable(
                    id='fight-table',
                    columns=[{"name": col.replace('_', ' ').title(), "id": col} for col in table_columns],
                    data=table_data,
                    style_table={'overflowX': 'auto'},
                    style_cell={
                        'textAlign': 'left',
                        'padding': '10px',
                        'minWidth': '100px',
                        'width': '100px',
                        'maxWidth': '150px',
                        'whiteSpace': 'normal'
                    },
                    style_header={
                        'backgroundColor': 'rgb(230, 230, 230)',
                        'fontWeight': 'bold'
                    },
                    style_data_conditional=[
                        {
                            'if': {
                                'column_id': 'result',
                                'filter_query': '{result} eq "Win"'
                            },
                            'color': 'green'
                        },
                        {
                            'if': {
                                'column_id': 'result',
                                'filter_query': '{result} eq "Loss"'
                            },
                            'color': 'red'
                        }
                    ],
                    page_size=25,
                )
            ])
        ],
        className="mb-4 shadow"
    )

# --------------- Modularized Callback ---------------
@app.callback(
    Output('fighter-profile', 'children'),
    Input('fighter-dropdown', 'value')
)
def update_profile(selected_fighter_url):
    if not selected_fighter_url:
        return html.Div("Please select a fighter from the dropdown above.", className="mt-4 text-center")
    
    fighter = df_current[df_current['fighter_url'] == selected_fighter_url].iloc[0]
    fighter_fights_all = df[df['fighter_url'] == selected_fighter_url].sort_values(by='date', ascending=False).copy()
    
    stats = compute_additional_stats(fighter, fighter_fights_all)
    # Compute weight class distribution for the selected fighter
    weight_distribution = compute_weight_class_distribution(fighter_fights_all)
    
    info_card = build_fighter_info_card(fighter, stats)
    stats_card = build_stats_card(fighter, weight_distribution)
    table_card = build_fight_history_table(fighter_fights_all)
    
    return dbc.Container([
        dbc.Row([dbc.Col(info_card, md=4), dbc.Col(stats_card, md=8)]),
        dbc.Row([dbc.Col(table_card, md=12)])
    ], fluid=True)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run_server(host='0.0.0.0', port=port, debug=True)
