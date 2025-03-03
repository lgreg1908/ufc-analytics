import pandas as pd
import plotly.graph_objs as go
import plotly.express as px
from typing import Optional, Dict, Any

# --------------- Histogram Function ---------------
def build_histogram(
    series: pd.Series, 
    nbins: int = 40, 
    width: int = 1000, 
    height: int = 500,
    label: str = 'Value', 
    title: str = 'Distribution', 
    highlight_value: Optional[float] = None
) -> go.Figure:
    """
    Create a histogram for a given numeric pandas Series with an optional vertical highlight line.

    Args:
        series (pd.Series): The numerical data to be visualized in the histogram.
        nbins (int, optional): The number of bins in the histogram. Default is 40.
        width (int, optional): The width of the figure in pixels. Default is 1000.
        height (int, optional): The height of the figure in pixels. Default is 500.
        label (str, optional): The label for the x-axis. Default is 'Value'.
        title (str, optional): The title of the histogram. Default is 'Distribution'.
        highlight_value (Optional[float], optional): A specific value to highlight with a vertical dashed line. Default is None.

    Returns:
        go.Figure: A Plotly figure representing the histogram.
    """
    fig = px.histogram(
        x=series,
        nbins=nbins,
        labels={'x': label},
        title=title
    )
    fig.update_layout(width=width, height=height)
    
    if highlight_value is not None:
        fig.add_shape(
            type='line',
            x0=highlight_value,
            x1=highlight_value,
            yref='paper',
            y0=0,
            y1=1,
            line=dict(color='orange', width=3, dash='dash')
        )
    
    return fig

def build_fight_outcome_chart(fighter: Dict[str, Any]) -> go.Figure:
    """
    Create a grouped bar chart showing the fighter's win-loss breakdown by outcome type.

    Args:
        fighter (Dict[str, Any]): A dictionary containing the fighter's fight statistics. 
                                  Expected keys:
                                  - 'total_win_knockout' (int): Number of wins by knockout.
                                  - 'total_win_submission' (int): Number of wins by submission.
                                  - 'total_win_decision' (int): Number of wins by decision.
                                  - 'total_loss_knockout' (int): Number of losses by knockout.
                                  - 'total_loss_submission' (int): Number of losses by submission.
                                  - 'total_loss_decision' (int): Number of losses by decision.

    Returns:
        go.Figure: A Plotly figure representing the grouped bar chart of fight outcomes.
    """
    outcome_categories = ['Knockout', 'Submission', 'Decision']
    
    # Extract win and loss statistics, defaulting to 0 if the key is missing
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
