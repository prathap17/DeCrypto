from flask import *
import pandas as pd
from cassandra_connection import FetchData

app = Flask(__name__)


@app.route("/tables")
def show_tables():
    session = FetchData().start_connection()
    query1 = FetchData().prepare_asks_query(session)
    query2 = FetchData().prepare_bids_query(session)
    asks = FetchData().get_data_asks(query1, session)
    bids = FetchData().get_data_bids(query2, session)
    return render_template(
        'view.html',
        tables=[asks.to_html(classes='asks'),
                bids.to_html(classes='bids')],
        titles=['na', 'ASKS', 'BIDS'])


if __name__ == "__main__":
    app.run(host="", port=80, debug=True)