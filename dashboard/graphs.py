import plotly.express as px


def depreciation_chart(
    df,
    product_column,
    age_column,
    price_column
):

    fig = px.line(
        df,
        x=age_column,
        y=price_column,
        color=product_column,
        markers=True,
        title="Product Depreciation Curve"
    )

    fig.update_layout(
       
