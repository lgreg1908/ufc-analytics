import os
import sys

# Add the parent directory of the current file (i.e., project-root) to the PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import datetime
import pandas as pd
import dash
from dash import dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objs as go

from pipeline.src.utils import load_yaml, load_parquet_from_gcs
from pipeline.src.transform.results_utils import wide_to_long_results
from pipeline.src.transform.utils import add_all_cumsum_columns, subset_most_recent_fight

def read_data(config: dict) -> pd.DataFrame:
    df_results_clean = load_parquet_from_gcs(
        blob_name=config['output_files']['clean']['results'],
        bucket_name=config['gcs']['bucket'])
    df_fighters_clean = load_parquet_from_gcs(
        blob_name=config['output_files']['clean']['fighters'],
        bucket_name=config['gcs']['bucket'])
    df_events_clean = load_parquet_from_gcs(
        blob_name=config['output_files']['clean']['events'],
        bucket_name=config['gcs']['bucket'])
    
    df_fighters_clean_opp = (
        df_fighters_clean[['fighter_url', 'full_name']]
        .rename(columns={"fighter_url": "opp_url", "full_name": "opp_full_name"}))
    
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

# Full DataFrame with computed stats
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

# Create a grouped bar chart for fight outcomes (wins vs losses)
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

# Initialize the Dash app with Bootstrap styling
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "MMA Fighter Dashboard"

app.layout = dbc.Container([
    html.H1("MMA Fighter Dashboard", className="my-4 text-center"),
    
    dbc.Row([
        dbc.Col(
            dcc.Dropdown(
                id='fighter-dropdown',
                options=[
                    {'label': row['full_name'], 'value': row['fighter_url']}
                    for _, row in df_current.iterrows()
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

@app.callback(
    Output('fighter-profile', 'children'),
    Input('fighter-dropdown', 'value')
)
def update_profile(selected_fighter_url):
    if not selected_fighter_url:
        return html.Div("Please select a fighter from the dropdown above.", className="mt-4 text-center")
    
    # Retrieve fighter record (for overall stats) and filter fights for table
    fighter = df_current[df_current['fighter_url'] == selected_fighter_url].iloc[0]
    fighter_fights = df[df['fighter_url'] == selected_fighter_url].sort_values(by='date', ascending=False).copy()
    
    # Format the date nicely for the profile card and table
    fight_date = fighter['date'].strftime('%B %d, %Y') if hasattr(fighter['date'], 'strftime') else fighter['date']
    fighter_fights['date'] = fighter_fights['date'].apply(lambda d: d.strftime('%B %d, %Y') if hasattr(d, 'strftime') else d)
    
    # Compute UFC record from cumulative stats
    ufc_record = f"{fighter.get('total_win', 0)}-{fighter.get('total_loss', 0)}-{fighter.get('total_draw', 0)}"
    
    # Convert total fight duration (seconds) to HH:MM:SS format
    total_fight_time = str(datetime.timedelta(seconds=int(fighter.get('total_fight_duration_seconds', 0))))
    
    # Build the Fighter Info Card
    info_card = dbc.Card(
        [
            dbc.CardHeader(html.H3(fighter['full_name'], className="text-center")),
            dbc.CardBody([
                html.H5("Fighter Info", className="card-title"),
                html.P(f"Nickname: {fighter['nickname']}" if fighter['nickname'] else "Nickname: N/A"),
                html.P(f"Overall Record: {fighter['record']}"),
                html.P(f"UFC Record: {ufc_record}"),
                html.Hr(),
                html.H6("Physical Attributes:"),
                html.P(f"Height: {fighter['height']}"),
                html.P(f"Reach: {fighter['reach']}"),
                html.P(f"Stance: {fighter['stance']}"),
                html.Hr(),
                html.H6("Recent Fight Details:"),
                html.P(f"Outcome: {fighter['result']}"),
                html.P(f"Event: {fighter['event']}"),
                html.P(f"Date: {fight_date}")
            ])
        ],
        className="mb-4 shadow"
    )
    
    # Build the Stats & Bonuses Card
    stats_card = dbc.Card(
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
                        html.P(total_fight_time)
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
    
    # Prepare Fight History Table
    # Create a friendly "Fight Duration" column by converting seconds to HH:MM:SS
    fighter_fights['fight_duration'] = fighter_fights['fight_duration_seconds'].apply(
        lambda s: str(datetime.timedelta(seconds=int(s)))
    )
    # Define the columns to display
    table_columns = ["date", "event", "weight_class", "opp_full_name","method", "round", "time", "result",
                      "fight_duration", 'fight_duration_seconds',
                     "title_fight", "perf_bonus", "fight_of_the_night"]
    table_data = fighter_fights[table_columns].to_dict('records')
    
    fight_table = dash_table.DataTable(
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
        
    table_card = dbc.Card(
        [
            dbc.CardHeader(html.H4("Fight History", className="text-center")),
            dbc.CardBody([fight_table])
        ],
        className="mb-4 shadow"
    )
    
    # Arrange the layout: top row with two cards, then the fight history table in a new row.
    return dbc.Container([
        dbc.Row([
            dbc.Col(info_card, md=4),
            dbc.Col(stats_card, md=8)
        ]),
        dbc.Row([
            dbc.Col(table_card, md=12)
        ])
    ], fluid=True)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)

