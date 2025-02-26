import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import dash
from dash import dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import plotly.express as px

# Add the project root to PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pipeline.src.utils import load_yaml, load_parquet_from_gcs
from pipeline.src.transform.results_utils import wide_to_long_results
from pipeline.src.transform.utils import add_all_cumsum_columns, subset_most_recent_fight

# --------------- Data Loading ---------------
def read_data(config: dict) -> pd.DataFrame:
    df_results_clean = load_parquet_from_gcs(
        blob_name=config['output_files']['clean']['results'],
        bucket_name=config['gcs']['bucket']
    )
    df_fighters_clean = load_parquet_from_gcs(
        blob_name=config['output_files']['clean']['fighters'],
        bucket_name=config['gcs']['bucket']
    )
    df_events_clean = load_parquet_from_gcs(
        blob_name=config['output_files']['clean']['events'],
        bucket_name=config['gcs']['bucket']
    )
    
    df_fighters_clean_opp = (
        df_fighters_clean[['fighter_url', 'full_name']]
        .rename(columns={"fighter_url": "opp_url", "full_name": "opp_full_name"})
    )
    
    df_results_long = df_results_clean.pipe(wide_to_long_results)
    df = (
        df_results_long
        .merge(df_fighters_clean, on='fighter_url')
        .merge(df_fighters_clean_opp, on='opp_url')
        .merge(df_events_clean, on='event_url')
        .sort_values(by=['date', 'fight_url', 'fighter_url'])
        .reset_index(drop=True)
    )
    return df

# Build full dataframe and computed stats
df = (
    read_data(config=load_yaml(os.path.join('pipeline', 'config', 'config.yaml')))
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

# --------------- Overall Distribution Calculation ---------------
def summarize_series(series: pd.Series) -> str:
    """Return a summary string (mean, median, std, range) for the given series."""
    mean_val = series.mean()
    median_val = series.median()
    std_val = series.std()
    min_val = series.min()
    max_val = series.max()
    return f"Mean: {mean_val:.1f} days, Median: {median_val:.1f} days, Std: {std_val:.1f} days, Range: {min_val:.0f}-{max_val:.0f} days"

def compute_fighter_avg_interval(df):
    """Compute the average time between fights for each fighter."""
    def avg_interval(dates):
        if len(dates) > 1:
            sorted_dates = sorted(dates)
            total_span = sorted_dates[-1] - sorted_dates[0]
            return total_span.days / (len(sorted_dates) - 1)
        return None
    return df.groupby('fighter_url')['date'].apply(lambda x: avg_interval(x.tolist())).dropna()

overall_avg_intervals = compute_fighter_avg_interval(df)
distribution_summary = summarize_series(overall_avg_intervals)

# Create histogram figure for overall average intervals with more granularity and a wider layout
hist_fig = px.histogram(
    x=overall_avg_intervals, 
    nbins=40,  # Increase the number of bins for a more granular view
    labels={'x': 'Avg Interval (days)'}, 
    title="Distribution of Average Time Between Fights"
)
hist_fig.update_layout(width=1000, height=500)  # Make the histogram wider and set a custom height


# --------------- Visualization ---------------
def create_stats_figure(fighter):
    outcome_categories = ['Knockout', 'Submission', 'Decision']
    wins = [
        fighter.get('total_win_knockout', 0),
        fighter.get('total_win_submission', 0),
        fighter.get('total_win_decision', 0)
    ]
    losses = [
        fighter.get('total_loss_knockout', 0),
        fighter.get('total_loss_submission', 0),
        fighter.get('total_loss_decision', 0)
    ]
    fig = go.Figure(data=[
        go.Bar(name='Wins', x=outcome_categories, y=wins, marker_color='green'),
        go.Bar(name='Losses', x=outcome_categories, y=losses, marker_color='red')
    ])
    fig.update_layout(
        barmode='group',
        title="Fight Outcome Breakdown",
        xaxis_title="Outcome Type",
        yaxis_title="Count",
        template="plotly_white",
        margin=dict(l=20, r=20, t=40, b=20)
    )
    return fig

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

# --------------- Helper Functions for Calculations ---------------
def calculate_age(birth_date):
    today = datetime.today()
    return (today - birth_date).days // 365

def calculate_time_since_last_fight(last_fight_date):
    today = datetime.today()
    return today - last_fight_date

def calculate_average_time_between_fights(fight_dates):
    if len(fight_dates) <= 1:
        return None
    sorted_dates = sorted(fight_dates)
    total_span = sorted_dates[-1] - sorted_dates[0]
    return total_span / (len(sorted_dates) - 1)

def compute_additional_stats(fighter, fights_df):
    age = calculate_age(fighter['date_of_birth'])
    time_since_last = calculate_time_since_last_fight(fighter['date'])
    fight_dates = fights_df['date'].tolist()
    avg_interval = calculate_average_time_between_fights(fight_dates)
    return {
        'age': age,
        'time_since_last_days': time_since_last.days,
        'avg_interval_days': avg_interval.days if avg_interval is not None else "N/A"
    }

# --------------- Helper Functions for UI Components ---------------
def build_fighter_info_card(fighter, stats):
    fight_date_disp = fighter['date'].strftime('%B %d, %Y') if hasattr(fighter['date'], 'strftime') else fighter['date']
    ufc_record = f"{fighter.get('total_win', 0)}-{fighter.get('total_loss', 0)}-{fighter.get('total_draw', 0)}"
    
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
                html.P(f"Height: {fighter['height']} ({fighter['height_cm']} cm)"),
                html.P(f"Reach: {fighter['reach']} ({fighter['reach_cm']} cm)"),
                html.P(f"Stance: {fighter['stance']}"),
                html.Hr(),
                # Career Metrics with Popover for Histogram
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
                                id="histogram-graph",
                                figure=hist_fig,
                                config={"displayModeBar": False}
                            )
                        ),
                    ],
                    id="avg-interval-popover",
                    target="avg-interval-target",
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

def build_stats_card(fighter):
    return dbc.Card(
        [
            dbc.CardHeader(html.H4("Cumulative Statistics", className="text-center")),
            dbc.CardBody([
                dcc.Graph(
                    id="stats-graph",
                    figure=create_stats_figure(fighter)
                ),
                html.Hr(),
                dbc.Row([
                    dbc.Col(html.Div([
                        html.H6("Total Fight Time:"),
                        html.P(str(timedelta(seconds=int(fighter.get('total_fight_duration_seconds', 0)))))
                    ]), md=4),
                    dbc.Col(html.Div([
                        html.H6("Title Fights:"),
                        html.P(fighter.get('total_title_fight', 0))
                    ]), md=4),
                    dbc.Col(html.Div([
                        html.H6("Performance Bonuses:"),
                        html.P(fighter.get('total_perf_bonus', 0))
                    ]), md=4)
                ], className="mb-3"),
                dbc.Row([
                    dbc.Col(html.Div([
                        html.H6("Fight of the Night:"),
                        html.P(fighter.get('total_fight_of_the_night', 0))
                    ]), md=4)
                ])
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
                    page_size=10,
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
    
    info_card = build_fighter_info_card(fighter, stats)
    stats_card = build_stats_card(fighter)
    table_card = build_fight_history_table(fighter_fights_all)
    
    return dbc.Container([
        dbc.Row([dbc.Col(info_card, md=4), dbc.Col(stats_card, md=8)]),
        dbc.Row([dbc.Col(table_card, md=12)])
    ], fluid=True)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run_server(host='0.0.0.0', port=port, debug=True)
